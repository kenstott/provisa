<?php
// The test identity provider's users (REQ-1265, tests/integration/test_saml_auth_provider.py).
// Mounted over the image's own authsources.php, whose users carry only an email.
$config = array(
    'admin' => array(
        'core:AdminPassword',
    ),
    'example-userpass' => array(
        'exampleauth:UserPass',
        'user1:password' => array(
            'uid' => array('user1'),
            'email' => 'user1@example.com',
            'eduPersonAffiliation' => array('group1'),
        ),
        'user2:password' => array(
            'uid' => array('user2'),
            'email' => 'user2@example.com',
            'eduPersonAffiliation' => array('group2'),
        ),
    ),
);
