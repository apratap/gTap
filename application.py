from app.archive_agent import ArchiveAgent
from app.context import create_database, add_log_entry
import app.search_consent as search_consent
import app.config as config


create_database(config.DATABASE)

add_log_entry('starting archive agent')
agent = ArchiveAgent(conn=config.DATABASE, wait_time=0)
agent.start_async()
add_log_entry(agent.get_status())

if __name__ == '__main__':
    application = search_consent.create_app(config, ssl=False, debug=True)
    application.run(host='localhost', port=8080)
else:
    application = search_consent.create_app(config)
