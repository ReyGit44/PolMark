"""
Authentication manager for Polymarket CLOB API.
Handles L1 (EIP-712) and L2 (HMAC) authentication.
"""

import base64
import hashlib
import hmac
import time
from typing import Optional

from eth_account import Account
from eth_account.messages import encode_typed_data


class AuthManager:
    """Manages authentication for Polymarket API."""
    
    CLOB_AUTH_DOMAIN = {
        "name": "ClobAuthDomain",
        "version": "1",
        "chainId": 137,
    }
    
    CLOB_AUTH_TYPES = {
        "ClobAuth": [
            {"name": "address", "type": "address"},
            {"name": "timestamp", "type": "string"},
            {"name": "nonce", "type": "uint256"},
            {"name": "message", "type": "string"},
        ]
    }
    
    AUTH_MESSAGE = "This message attests that I control the given wallet"
    
    def __init__(
        self,
        private_key: str,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        api_passphrase: Optional[str] = None,
        chain_id: int = 137,
    ):
        self.private_key = private_key
        self.account = Account.from_key(private_key)
        self.address = self.account.address
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
        self.chain_id = chain_id
        
        # Update domain with correct chain ID
        self.CLOB_AUTH_DOMAIN = {
            "name": "ClobAuthDomain",
            "version": "1",
            "chainId": chain_id,
        }
    
    def get_l1_headers(self, nonce: int = 0) -> dict[str, str]:
        """
        Generate L1 authentication headers using EIP-712 signature.
        Used for creating/deriving API credentials.
        """
        timestamp = str(int(time.time()))
        
        message_data = {
            "address": self.address,
            "timestamp": timestamp,
            "nonce": nonce,
            "message": self.AUTH_MESSAGE,
        }
        
        # Create EIP-712 typed data
        typed_data = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                ],
                **self.CLOB_AUTH_TYPES,
            },
            "primaryType": "ClobAuth",
            "domain": self.CLOB_AUTH_DOMAIN,
            "message": message_data,
        }
        
        # Sign the typed data
        signable = encode_typed_data(full_message=typed_data)
        signed = self.account.sign_message(signable)
        
        return {
            "POLY_ADDRESS": self.address,
            "POLY_SIGNATURE": "0x" + signed.signature.hex(),
            "POLY_TIMESTAMP": timestamp,
            "POLY_NONCE": str(nonce),
        }
    
    def get_l2_headers(
        self,
        method: str,
        path: str,
        body: str = "",
    ) -> dict[str, str]:
        """
        Generate L2 authentication headers using HMAC-SHA256.
        Used for authenticated API requests.
        """
        if not all([self.api_key, self.api_secret, self.api_passphrase]):
            raise ValueError("API credentials required for L2 authentication")
        
        timestamp = str(int(time.time()))
        
        # Create signature payload
        message = timestamp + method.upper() + path + body
        
        # Sign with HMAC-SHA256
        secret_bytes = base64.urlsafe_b64decode(self.api_secret)
        signature = hmac.new(
            secret_bytes,
            message.encode("utf-8"),
            hashlib.sha256
        ).digest()
        signature_b64 = base64.b64encode(signature).decode("utf-8")
        
        return {
            "POLY_ADDRESS": self.address,
            "POLY_SIGNATURE": signature_b64,
            "POLY_TIMESTAMP": timestamp,
            "POLY_API_KEY": self.api_key,
            "POLY_PASSPHRASE": self.api_passphrase,
        }
    
    def set_api_credentials(
        self,
        api_key: str,
        api_secret: str,
        api_passphrase: str,
    ) -> None:
        """Set API credentials after derivation."""
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_passphrase = api_passphrase
    
    def has_l2_credentials(self) -> bool:
        """Check if L2 credentials are available."""
        return all([self.api_key, self.api_secret, self.api_passphrase])
