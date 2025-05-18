const http = require("http");
const httpProxy = require('http-proxy');

const MONOLITH_URL = process.env.MONOLITH_URL;
const MOVIES_SERVICE_URL = process.env.MOVIES_SERVICE_URL;
const FEATURE_ENABLED = process.env.GRADUAL_MIGRATION === "true";
const MOVIES_MIGRATION_PERCENT = process.env.MOVIES_MIGRATION_PERCENT;


const config = [
    {
        startsWith: "/api/movies",
        target: MOVIES_SERVICE_URL,
    }
]

const proxy = httpProxy.createProxyServer();

const server = http.createServer((req, res) => {
    const target = getTarget(req);
    proxy.web(req, res, { target });
});

server.listen(process.env.PORT, () => {
  console.log(`Server is running on port ${process.env.PORT}`);
});

function getTarget(req) {
    const shouldUseFeature = isTestGroup() && FEATURE_ENABLED;
    const path = req.url;
    const target = config.find(c => path.startsWith(c.startsWith));

    if(!target || !shouldUseFeature) {
        return MONOLITH_URL;
    }

    return target.target;
}

function isTestGroup() {
    return Math.random() < MOVIES_MIGRATION_PERCENT / 100;
}