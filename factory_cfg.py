from bench import AwsBenchInstance, AzureBenchInstance

supported_providers = {
        'AWS_SPOT' : AwsBenchInstance,
        'AZURE' : AzureBenchInstance
    }