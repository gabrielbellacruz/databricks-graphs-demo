import { createApp, analytics, genie, server } from '@databricks/appkit';

createApp({
  plugins: [
    analytics(),
    // Lê DATABRICKS_GENIE_SPACE_ID (injetado pelo recurso genie-space do databricks.yml) e registra o alias "default"
    genie(),
    server(),
  ],
  onPluginsReady(appkit) {
    appkit.server.extend((app) => {
      // Identidade do analista: cabeçalhos injetados pela plataforma Databricks Apps
      app.get('/api/whoami', (req, res) => {
        res.json({
          email: req.header('x-forwarded-email') ?? null,
          user: req.header('x-forwarded-user') ?? null,
        });
      });
    });
  },
}).catch(console.error);
