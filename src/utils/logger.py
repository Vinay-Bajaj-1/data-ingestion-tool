import logging
import os


class AppLogger:
    """
    A static utility class to configure and provide a named logger instance
    for each module/component of the application.
    Each logger will write to a dedicated log file in the 'logs' directory.
    """

    @staticmethod
    def get_logger(name, log_dir = 'logs', level=logging.ERROR):
        """
        Retrieves or creates a named logger instance, configuring it to write
        to a specific log file in the 'logs' directory.

        Args:
            name (str): The name of the logger (e.g., '__name__' of the module).
                        This name will also be used to derive the log file name.
            log_dir (str): The directory where logs should be stored, relative to the project root.
            level (int): The minimum logging level for this logger.

        Returns:
            logging.Logger: The configured logger instance.
        """
        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.propagate = False # Crucial: Prevent messages from going to the root logger

        # Determine the project root dynamically
        current_file_dir = os.path.dirname(os.path.abspath(__file__))
        src_dir = os.path.dirname(current_file_dir)
        project_root = os.path.dirname(src_dir)

        full_log_dir_path = os.path.join(project_root, log_dir)

        # Create the log directory if it doesn't exist
        if not os.path.exists(full_log_dir_path):
            os.makedirs(full_log_dir_path)

        log_file_name = f"{name.replace('.', '_')}.log"
        log_file_path = os.path.join(full_log_dir_path, log_file_name)

        # Add handlers only if they don't already exist for this logger
        # This prevents duplicate log entries if get_logger is called multiple times for the same name
        if not logger.handlers:
            # File handler
            file_handler = logging.FileHandler(log_file_path, mode='a', encoding='utf-8')
            file_handler.setLevel(level)
            # Use '%(name)s' in the formatter to show the logger's name in the log file
            file_formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(name)s:%(lineno)d - %(message)s')

            file_handler.setFormatter(file_formatter)
            logger.addHandler(file_handler)

            # Console handler (optional, but good for real-time feedback)
            console_handler = logging.StreamHandler()
            console_handler.setLevel(level)
            console_formatter = logging.Formatter('[%(asctime)s] %(levelname)s - %(name)s:%(lineno)d - %(message)s')
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)

        return logger
