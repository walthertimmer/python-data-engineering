import logging
import sys
from datetime import datetime

def setup_logging(log_level: str = "INFO") -> None:
    """Configure logging with timestamp, level and message"""
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def main() -> None:
    """Main function"""
    try:
        # Setup logging
        setup_logging()
        logger = logging.getLogger(__name__)
        
        logger.info("Starting process")
        start_time = datetime.now()
        
        # main logic
        logger.info("Hello, World!")
        
        # completion
        duration = datetime.now() - start_time
        logger.info(f"Process completed successfully in {duration}")
        sys.exit(0)

    except Exception as e:
        logger.error(f"Process failed: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()