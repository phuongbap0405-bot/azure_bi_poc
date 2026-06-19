const { app } = require("@azure/functions");
const { DefaultAzureCredential } = require("@azure/identity");
const { SecretClient } = require("@azure/keyvault-secrets");

app.http("HttpTest", {
    methods: ["GET"],
    authLevel: "function",
    handler: async (request, context) => {
        try {
            const keyVaultUrl = process.env.KEY_VAULT_URL;
            const secretName = process.env.CONTROLIO_TOKEN_SECRET_NAME;

            if (!keyVaultUrl || !secretName) {
                return {
                    status: 500,
                    jsonBody: {
                        success: false,
                        message: "Missing Key Vault environment variables",
                    },
                };
            }

            const credential = new DefaultAzureCredential();
            const client = new SecretClient(keyVaultUrl, credential);

            const secret = await client.getSecret(secretName);

            return {
                status: 200,
                jsonBody: {
                    success: true,
                    message: "Successfully read the secret from Key Vault",
                    secretName: secret.name,
                    secretVersion: secret.properties.version,
                },
            };
        } catch (error) {
            context.error("Key Vault test failed", error);

            return {
                status: 500,
                jsonBody: {
                    success: false,
                    message: error.message,
                },
            };
        }
    },
});