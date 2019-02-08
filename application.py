import sys

from app.aa import ArchiveAgent
import app.search_consent as search_consent
import app.config as config

agent = ArchiveAgent(config.DATABASE)
agent.start_async()
sys.stdout.write('\narchive agent started\n')

application = search_consent.create_app(config)

if __name__ == '__main__':
    application.run()
