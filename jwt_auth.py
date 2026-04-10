import os
import jwt
import logging
from datetime import datetime
from typing import Optional, Dict
from functools import wraps
import streamlit as st
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger("jwt_auth")

# JWT Configuration
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "your-secret-key-here")
JWT_ALGORITHM = "HS256"
JWT_ISSUER = os.getenv("JWT_ISSUER", "https://localhost:7048/")
JWT_AUDIENCE = os.getenv("JWT_AUDIENCE", "https://localhost:4200/")


class JWTAuthenticator:
    """JWT Authentication Manager for CorxAi SQL Assistant"""

    def __init__(self):
        self.secret_key = JWT_SECRET_KEY
        self.algorithm = JWT_ALGORITHM
        self.issuer = JWT_ISSUER
        self.audience = JWT_AUDIENCE

    def decode_token(self, token: str) -> Optional[Dict]:
        """
        Decode and validate JWT token
        
        Args:
            token: JWT token string
            
        Returns:
            Dictionary with user claims if valid, None if invalid
        """
        try:
            if token.startswith('Bearer '):
                token = token[7:]

            payload = jwt.decode(
                token,
                self.secret_key,
                algorithms=[self.algorithm],
                issuer=self.issuer,
                audience=self.audience,
                options={
                    "verify_signature": True,
                    "verify_exp": True,
                    "verify_iss": True,
                    "verify_aud": True
                }
            )

            user_data = {
                "user_id": payload.get("http://schemas.xmlsoap.org/ws/2005/05/identity/claims/nameidentifier"),
                "user_name": payload.get("userName"),
                "user_code": payload.get("userCode"),
                "plantCode": payload.get("plantCode"),
                "group_code": payload.get("groupCode"),
                "role_id": payload.get("roleId"),
                "role_name": payload.get("roleName"),
                "exp": payload.get("exp"),
                "iss": payload.get("iss"),
                "aud": payload.get("aud"),
                "raw_payload": payload
            }

            logger.info(
                f"✅ Token decoded successfully for user: {user_data['user_name']} (Plant: {user_data['plantCode']})")
            return user_data

        except jwt.ExpiredSignatureError:
            logger.error("❌ Token has expired")
            return None
        except jwt.InvalidIssuerError:
            logger.error("❌ Invalid token issuer")
            return None
        except jwt.InvalidAudienceError:
            logger.error("❌ Invalid token audience")
            return None
        except jwt.InvalidSignatureError:
            logger.error("❌ Invalid token signature")
            return None
        except Exception as e:
            logger.error(f"❌ Token decode error: {e}")
            return None

    def decode_token_without_verification(self, token: str) -> Optional[Dict]:
        """
        Decode token without signature verification (for development/debugging)
        
        Args:
            token: JWT token string
            
        Returns:
            Dictionary with user claims if decodable, None if invalid
        """
        try:
            if token.startswith('Bearer '):
                token = token[7:]

            payload = jwt.decode(
                token,
                options={"verify_signature": False}
            )

            user_data = {
                "user_id": payload.get("http://schemas.xmlsoap.org/ws/2005/05/identity/claims/nameidentifier"),
                "user_name": payload.get("userName"),
                "user_code": payload.get("userCode"),
                "plantCode": payload.get("plantCode"),
                "group_code": payload.get("groupCode"),
                "role_id": payload.get("roleId"),
                "role_name": payload.get("roleName"),
                "exp": payload.get("exp"),
                "raw_payload": payload
            }

            logger.info(
                f"🔓 Token decoded (no verification) for user: {user_data['user_name']}")
            return user_data

        except Exception as e:
            logger.error(f"❌ Token decode error: {e}")
            return None

    def validate_user_access(self, user_data: Dict, required_plantCode: Optional[str] = None) -> bool:
        """
        Validate if user has access based on plant code
        
        Args:
            user_data: Decoded user data from token
            required_plantCode: Optional specific plant code to check
            
        Returns:
            True if user has access, False otherwise
        """
        if not user_data:
            return False

        if not user_data.get("plantCode"):
            logger.warning("⚠️ User has no plant code")
            return False

        if required_plantCode and user_data.get("plantCode") != required_plantCode:
            logger.warning(
                f"⚠️ Plant code mismatch: {user_data.get('plantCode')} != {required_plantCode}")
            return False

        return True

    def get_user_info_display(self, user_data: Dict) -> str:
        """
        Generate formatted user info string for display
        
        Args:
            user_data: Decoded user data from token
            
        Returns:
            Formatted string with user information
        """
        if not user_data:
            return "❌ Not authenticated"

        exp_timestamp = user_data.get("exp", 0)
        exp_datetime = datetime.fromtimestamp(
            exp_timestamp) if exp_timestamp else None

        info = f"""
**👤 User:** {user_data.get('user_name', 'N/A')}
**🏭 Plant:** {user_data.get('plantCode', 'N/A')}
**🔑 User Code:** {user_data.get('user_code', 'N/A')}
**👥 Role:** {user_data.get('role_name', 'N/A')}
**⏰ Token Expires:** {exp_datetime.strftime('%Y-%m-%d %H:%M:%S') if exp_datetime else 'N/A'}
"""
        return info


def initialize_auth_session():
    """Initialize authentication session state"""
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False
    if "user_data" not in st.session_state:
        st.session_state.user_data = None
    if "jwt_token" not in st.session_state:
        st.session_state.jwt_token = None
    if "token_checked" not in st.session_state:
        st.session_state.token_checked = False


def get_token_from_url() -> Optional[str]:
    """
    Extract JWT token from URL query parameters
    
    Returns:
        JWT token string if found in URL, None otherwise
    """
    try:
        if hasattr(st, 'query_params'):
            query_params = st.query_params

            if "token" in query_params:
                token = query_params["token"]
                logger.info("🔍 Token found in URL query parameters (new API)")
                return token
        else:
            query_params = st.experimental_get_query_params()

            if "token" in query_params:
                token = query_params["token"][0] if isinstance(
                    query_params["token"], list) else query_params["token"]
                logger.info(
                    "🔍 Token found in URL query parameters (legacy API)")
                return token

        logger.info("📭 No token found in URL query parameters")
        return None

    except Exception as e:
        logger.error(f"❌ Error extracting token from URL: {e}")
        return None


def authenticate_from_url(verify_signature: bool = False) -> bool:
    """
    Automatically authenticate user from URL token
    
    Args:
        verify_signature: Whether to verify token signature (set False for development)
        
    Returns:
        True if authentication successful, False otherwise
    """
    if st.session_state.get("token_checked", False):
        return st.session_state.get("authenticated", False)

    st.session_state.token_checked = True

    token = get_token_from_url()

    if not token:
        logger.warning("⚠️ No token provided in URL")
        return False

    return authenticate_user(token, verify_signature)


def authenticate_user(token: str, verify_signature: bool = True) -> bool:
    """
    Authenticate user with JWT token
    
    Args:
        token: JWT token string
        verify_signature: Whether to verify token signature (set False for development)
        
    Returns:
        True if authentication successful, False otherwise
    """
    authenticator = JWTAuthenticator()

    if verify_signature:
        user_data = authenticator.decode_token(token)
    else:
        user_data = authenticator.decode_token_without_verification(token)

    if user_data:
        st.session_state.authenticated = True
        st.session_state.user_data = user_data
        st.session_state.jwt_token = token

        logger.info(
            f"✅ User authenticated: {user_data['user_name']} (Plant: {user_data['plantCode']})")
        return True
    else:
        st.session_state.authenticated = False
        st.session_state.user_data = None
        st.session_state.jwt_token = None

        logger.warning("❌ Authentication failed")
        return False


def logout_user():
    """Logout user and clear session"""
    st.session_state.authenticated = False
    st.session_state.user_data = None
    st.session_state.jwt_token = None
    st.session_state.token_checked = False
    logger.info("🚪 User logged out")


def require_auth(func):
    """Decorator to require authentication for functions"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not st.session_state.get("authenticated", False):
            st.error("🔒 Authentication required. Please login.")
            return None
        return func(*args, **kwargs)
    return wrapper


def get_user_plantCode() -> Optional[str]:
    """Get authenticated user's plant code"""
    if st.session_state.get("authenticated") and st.session_state.get("user_data"):
        return st.session_state.user_data.get("plantCode")
    return None


def get_user_code() -> Optional[str]:
    """Get authenticated user's user code"""
    if st.session_state.get("authenticated") and st.session_state.get("user_data"):
        return st.session_state.user_data.get("user_code")
    return None


def get_user_name() -> Optional[str]:
    """Get authenticated user's name"""
    if st.session_state.get("authenticated") and st.session_state.get("user_data"):
        return st.session_state.user_data.get("user_name")
    return None
