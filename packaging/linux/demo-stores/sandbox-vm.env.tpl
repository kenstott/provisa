PROVISA_DEMO_BUNDLES=${bundles}
DEMO_BUNDLES_DIR=/root/.provisa/sandbox-data/demo-bundles
DEMO_PULL=docker exec compose-provisa-sandbox-1 python -m provisa.core.demo_bundles
DEMO_PULL_INTO=/var/lib/provisa/demo-bundles
DEMO_STORES_NETWORK=compose_default
DEMO_STORES=/root/.provisa/bin/demo_stores.py
DEMO_STORES_SHARED=/root/.provisa/bin/demo-stores-shared.yml
DEMO_STORES_PROJECT=provisa-demo-stores
