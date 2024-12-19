import logging
import os
import sys
from datetime import datetime
from typing import Optional

def setup_logging(log_level: str = "INFO") -> None:
    """Configure logging with timestamp, level and message"""
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
def get_env_var(var_name: str, default: Optional[str] = None) -> str:
    """Get environment variable with optional default value"""
    value = os.environ.get(var_name, default)
    if value is None:
        raise ValueError(f"Environment variable {var_name} not set")
    return value

def main() -> None:
    """Main function"""
    try:
        # Setup logging
        setup_logging()
        logger = logging.getLogger(__name__)
        
        # Log script start
        logger.info("Starting process")
        start_time = datetime.now()

        # Get configuration from environment variables
        name = get_env_var("NAME","NAME env not set!")
        
        # Main logic
        logger.info(f"Hello, {name}!")

        # Log completion
        duration = datetime.now() - start_time
        logger.info(f"Process completed successfully in {duration}")
        sys.exit(0)

    except Exception as e:
        logger.error(f"Process failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
