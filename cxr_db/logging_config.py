import logging

def setup_logging(config):
    logging.basicConfig(
        level=config.get('level', 'INFO'),
        format=config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )