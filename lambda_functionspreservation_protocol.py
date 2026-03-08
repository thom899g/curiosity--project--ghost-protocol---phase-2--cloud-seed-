#!/usr/bin/env python3
"""
Preservation Protocol Lambda Function
Executes wallet monitoring and preservation actions with full error handling
and comprehensive logging for ecosystem observability.
"""

import os
import json
import logging
import traceback
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError
from web3 import Web3
from firebase_admin import firestore, initialize_app, credentials
from firebase_admin.exceptions import FirebaseError

# Initialize logging with AWS Lambda Powertools for structured logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
secrets_client = boto3.client('secretsmanager')
ssm_client = boto3.client('ssm')

# Firebase app initialization (lazy)
firebase_app = None
db = None

def get_firebase_client() -> firestore.Client:
    """Initialize and return Firebase Firestore client with singleton pattern."""
    global firebase_app, db
    
    if db is not None:
        return db
    
    try:
        # Get Firebase credentials from AWS Secrets Manager
        secret_response = secrets_client.get_secret_value(
            SecretId=os.environ['FIREBASE_SECRET_ARN']
        )
        firebase_creds = json.loads(secret_response['SecretString'])
        
        # Initialize Firebase
        cred = credentials.Certificate(firebase_creds)
        firebase_app = initialize_app(cred)
        db = firestore.client(app=firebase_app)
        
        logger.info("Firebase client initialized successfully")
        return db
        
    except (ClientError, FirebaseError, KeyError) as e:
        logger.error(f"Failed to initialize Firebase: {str(e)}")
        raise

def get_wallet_private_key() -> str:
    """Retrieve wallet private key from AWS Secrets Manager securely."""
    try:
        secret_response = secrets_client.get_secret_value(
            SecretId=os.environ['WALLET_SECRET_ARN']
        )
        return secret_response['SecretString']
        
    except ClientError as e:
        logger.error(f"Failed to retrieve wallet secret: {str(e)}")
        raise

def get_blockchain_rpc_url() -> str:
    """Get blockchain RPC URL from AWS Systems Manager Parameter Store."""
    try:
        response = ssm_client.get_parameter(
            Name=os.environ['RPC_URL_PARAM'],
            WithDecryption=True
        )
        return response['Parameter']['Value']
        
    except ClientError as e:
        logger.error(f"Failed to retrieve RPC URL: {str(e)}")
        raise

def check_wallet_balance(w3: Web3, wallet_address: str) -> Decimal:
    """
    Check wallet balance with comprehensive error handling.
    
    Args:
        w3: Web3 instance
        wallet_address: Wallet address to check
        
    Returns:
        Decimal balance in Ether
        
    Raises:
        Exception: If balance check fails
    """
    try:
        # Validate address format
        if not Web3.is_address(wallet_address):
            raise ValueError(f"Invalid address format: {wallet_address}")
        
        checksum_address = Web3.to_checksum_address(wallet_address)
        
        # Get balance with timeout
        balance_wei = w3.eth.get_balance(checksum_address)
        balance_ether = w3.from_wei(balance_wei, 'ether')
        
        logger.info(f"Wallet {checksum_address} balance: {balance_ether} ETH")
        return Decimal(str(balance_ether))
        
    except Exception as e:
        logger.error(f"Balance check failed: {str(e)}")
        raise

def execute_preservation_transaction(
    w3: Web3, 
    private_key: str, 
    to_address: str,
    amount_eth: Decimal
) -> Dict[str, Any]:
    """
    Execute preservation transaction to move funds to cold wallet.
    
    Args:
        w3: Web3 instance
        private_key: Wallet private key
        to_address: Destination cold wallet address
        amount_eth: Amount to transfer in ETH
        
    Returns:
        Transaction receipt dictionary
    """
    try:
        # Get wallet address from private key
        account = w3.eth.account.from_key(private_key)
        from_address = account.address
        
        # Convert amount to wei
        amount_wei = w3.to_wei(amount_eth, 'ether')
        
        # Get current gas parameters
        gas_price = w3.eth.gas_price
        nonce = w3.eth.get_transaction_count(from_address, 'pending')
        
        # Build transaction
        tx = {
            'nonce': nonce,
            'to': Web3.to_checksum_address(to_address),
            'value': amount_wei,
            'gas': 21000,  # Standard transfer gas
            'gasPrice': gas_price,
            'chainId': w3.eth.chain_id
        }
        
        # Sign transaction
        signed_tx = w3.eth.account.sign_transaction(tx, private_key)
        
        # Send transaction
        tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        
        # Wait for receipt (with timeout)
        tx_receipt = w3.eth.wait_for_transaction_receipt(tx_hash, timeout=120)
        
        logger.info(f"Transaction executed: {tx_hash.hex()}")
        return {
            'tx_hash': tx_hash.hex(),
            'status': tx_receipt.status,
            'block_number': tx_receipt.blockNumber,
            'gas_used': tx_receipt.gasUsed
        }
        
    except Exception as e:
        logger.error(f"Transaction execution failed: {str(e)}")
        raise

def log_to_firebase(
    db: firestore.Client,
    collection: str,
    data: Dict[str, Any],
    document_id: Optional[str] = None
) -> str:
    """
    Log data to Firebase with timestamp and metadata.
    
    Args:
        db: Firestore client
        collection: Collection name
        data: Data to log
        document_id: Optional document ID
        
    Returns:
        Document ID
    """
    try:
        # Add metadata
        enriched_data = {
            **data,
            'timestamp': datetime.now(timezone.utc),
            'source': 'preservation_protocol_lambda',
            'version': os.environ.get('LAMBDA_VERSION', 'unknown')
        }
        
        if document_id:
            doc_ref = db.collection(collection).document(document_id)
            doc_ref.set(enriched_data, merge=True)
        else:
            doc_ref = db.collection(collection).add(enriched_data)
            document_id = doc_ref[1].id if isinstance(doc_ref, tuple) else doc_ref.id
            
        logger.info(f"Logged to {collection}/{document_id}")
        return document_id
        
    except Exception as e:
        logger.error(f"Firebase logging failed: {str(e)}")
        raise

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler for preservation protocol.
    
    Args:
        event: Lambda event containing execution parameters
        context: Lambda context object
        
    Returns:
        Dict with execution results
    """
    execution_id = event.get('executionId', context.aws_request_id)
    
    try:
        logger.info(f"Starting preservation protocol execution: {execution_id}")
        
        # Initialize Firebase
        db = get_firebase_client()
        
        # Get configuration from environment
        threshold_eth = Decimal(os.environ.get('PRESERVATION_THRESHOLD_ETH', '1.0'))
        cold_wallet_address = os.environ['COLD_WALLET_ADDRESS']
        
        # Get wallet private key
        private_key = get_wallet_private_key()
        
        # Get RPC URL and initialize Web3
        rpc_url = get_blockchain_rpc_url()
        w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={'timeout': 30}))
        
        if not w3.is_connected():
            raise ConnectionError("Failed to connect to blockchain RPC")
        
        # Get wallet address from private key
        account = w3.eth.account.from_key(private_key)
        wallet_address = account.address
        
        # Check balance
        balance = check_wallet_balance(w3, wallet_address)
        
        # Log wallet state
        log_to_firebase(db, 'wallet_state', {
            'wallet_address': wallet_address,
            'balance_eth': float(balance),
            'threshold_eth': float(threshold_eth),
            'execution_id': execution_id
        }, document_id=wallet_address)
        
        result = {
            'executionId': execution_id,
            'walletAddress': wallet_address,
            'balanceEth': float(balance),
            'thresholdEth': float(threshold_eth),
            'preservationRequired': False,
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
        # Check if preservation is required
        if balance > threshold_eth:
            logger.info(f"Balance {balance} ETH exceeds threshold {threshold_eth} ETH")
            
            # Calculate amount to preserve (balance - threshold)
            preserve_amount = balance - threshold_eth
            
            # Execute preservation transaction
            tx_result = execute_preservation_transaction(
                w3, private_key, cold_wallet_address, preserve_amount
            )
            
            # Log transaction
            log_to_firebase(db, 'transactions', {
                'type': 'preservation',
                'from_address': wallet_address,
                'to_address': cold_wallet_address,
                'amount_eth': float(preserve_amount),
                'transaction_result': tx_result,
                'execution_id': execution_id
            })
            
            result.update({
                'preservationRequired': True,
                'preservationAmountEth': float(preserve_amount),
                'transactionHash': tx_result['tx_hash'],
                'transactionStatus': tx_result['status']
            })
            
            logger.info(f"Preservation executed: {tx_result['tx_hash']}")
            
        else:
            logger.info(f"Balance {balance} ETH below threshold {threshold_eth} ETH")
            
        return result
        
    except Exception as e:
        # Log error to Firebase
        error_data = {
            'execution_id': execution_id,
            'error': str(e),
            'stack_trace': traceback.format_exc(),
            'timestamp': datetime.now(timezone.utc)
        }
        
        try:
            if db:
                log_to_firebase(db, 'errors', error_data)
        except Exception as fb_error:
            logger.error(f"Failed to log error to Firebase: {fb_error}")
        
        logger.error(f