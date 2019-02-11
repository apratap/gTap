from app.aa import ArchiveAgent
import app.search_consent as search_consent
import app.config as config
import app.model as model
import os
import sys

app = search_consent.create_app(config)

# This is only used when running locally
if __name__ == '__main__':
    if os.path.exists(config.DATABASE['path']):
        os.remove(config.DATABASE['path'])

    model.create_database(config.DATABASE)

    agent = ArchiveAgent(config.DATABASE)
    agent.start_async()
    sys.stdout.write('archive agent started')

    app.run(host='localhost', port=8080, debug=True)
