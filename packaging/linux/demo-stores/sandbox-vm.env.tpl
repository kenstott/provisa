PROVISA_DEMO_BUNDLES=${bundles}
DEMO_BUNDLES_DIR=/root/.provisa/sandbox-data/demo-bundles
DEMO_PULL=docker exec compose-provisa-sandbox-1 python -m provisa.core.demo_bundles
DEMO_PULL_INTO=/var/lib/provisa/demo-bundles
DEMO_STORES_NETWORK=compose_default
