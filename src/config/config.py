config = {
    "openai": {
        "api_key": "xxxx"
    },
    "kafka": {
        "sasl.username": "xxxx",
        "sasl.password": "xxxx",
        "bootstrap.servers": "pkc-419q3.us-east4.gcp.confluent.cloud:9092",
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'session.timeout.ms': 50000
    },
    "schema_registry": {
            "url": "xxxx",
            "basic.auth.user.info": "xxxx"
    }
}
