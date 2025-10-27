#!/usr/bin/env node
/**
 * Comprehensive Jupiter API Domain and Endpoint Scanner
 * Tests all discovered Jupiter subdomains for working perps/lend/swap APIs
 */

const ALL_DOMAINS = [
  // Perps-specific
  'perps.jup.ag',
  'perps-api.jup.ag',
  'perps-data.jup.ag',
  'perp-api.jup.ag',
  'perps-keeper.jup.ag',

  // Main APIs
  'api.jup.ag',
  'lite-api.jup.ag',

  // Other discovered domains
  'secret.jup.ag',
  'coinbase-api.jup.ag',
  'airdrop-api.jup.ag',
  'jupuary-airdrop-api.jup.ag',
  'enjoyoors-api.jup.ag',
  'dev-customer.aws-beta-api.jup.ag',
  'free-dev-api.aws-beta-api.jup.ag',
  'jupuary-api.jup.ag',
];

const PERPS_PATHS = [
  // Standard REST paths
  { path: '/v1/markets', method: 'GET', desc: 'Markets V1' },
  { path: '/v1/pools', method: 'GET', desc: 'Pools V1' },
  { path: '/v1/positions', method: 'GET', desc: 'Positions V1 (no params)' },
  { path: '/v1/stats', method: 'GET', desc: 'Stats V1' },
  { path: '/v1/funding', method: 'GET', desc: 'Funding rates V1' },
  { path: '/v1/trades', method: 'GET', desc: 'Recent trades V1' },
  { path: '/v1/liquidations', method: 'GET', desc: 'Liquidations V1' },

  // Without version prefix
  { path: '/markets', method: 'GET', desc: 'Markets' },
  { path: '/pools', method: 'GET', desc: 'Pools' },
  { path: '/positions', method: 'GET', desc: 'Positions' },
  { path: '/stats', method: 'GET', desc: 'Stats' },
  { path: '/funding', method: 'GET', desc: 'Funding rates' },
  { path: '/trades', method: 'GET', desc: 'Trades' },

  // Perps namespace
  { path: '/perps/v1/markets', method: 'GET', desc: 'Perps/V1/Markets' },
  { path: '/perps/v1/pools', method: 'GET', desc: 'Perps/V1/Pools' },
  { path: '/perps/v1/positions', method: 'GET', desc: 'Perps/V1/Positions' },
  { path: '/perps/markets', method: 'GET', desc: 'Perps/Markets' },
  { path: '/perps/pools', method: 'GET', desc: 'Perps/Pools' },
  { path: '/perps/positions', method: 'GET', desc: 'Perps/Positions' },

  // Documentation/metadata
  { path: '/', method: 'GET', desc: 'Root' },
  { path: '/health', method: 'GET', desc: 'Health check' },
  { path: '/ping', method: 'GET', desc: 'Ping' },
  { path: '/docs', method: 'GET', desc: 'API docs' },
  { path: '/api-docs', method: 'GET', desc: 'API docs alt' },
  { path: '/openapi.json', method: 'GET', desc: 'OpenAPI spec' },
  { path: '/swagger.json', method: 'GET', desc: 'Swagger spec' },
];

const INTERESTING_STATUSES = new Set([200, 201, 400, 401, 403, 429]);

async function testEndpoint(domain, { path, method, desc }) {
  const url = `https://${domain}${path}`;

  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 8000);

    const response = await fetch(url, {
      method,
      headers: {
        'Accept': 'application/json',
        'User-Agent': 'Jupiter-Perps-Scanner/1.0',
        'Origin': 'https://jup.ag'
      },
      signal: controller.signal
    });

    clearTimeout(timeoutId);

    const contentType = response.headers.get('content-type') || '';
    let body = null;
    let bodyPreview = '';

    try {
      if (contentType.includes('application/json')) {
        body = await response.json();
        bodyPreview = JSON.stringify(body).substring(0, 500);
      } else {
        const text = await response.text();
        bodyPreview = text.substring(0, 500);
      }
    } catch (e) {
      bodyPreview = `[Parse error: ${e.message}]`;
    }

    return {
      url,
      domain,
      path,
      method,
      desc,
      status: response.status,
      ok: response.ok,
      contentType,
      bodyPreview,
      body,
      headers: Object.fromEntries(response.headers.entries())
    };
  } catch (error) {
    return {
      url,
      domain,
      path,
      method,
      desc,
      error: error.name === 'AbortError' ? 'Timeout' : error.message,
      status: null
    };
  }
}

async function main() {
  console.log('\n🔍 JUPITER API COMPREHENSIVE SCANNER');
  console.log('═══════════════════════════════════════════════════════════\n');

  const results = {
    working: [],      // 200-299
    interesting: [],  // 400, 401, 403, 429 (means endpoint exists)
    notFound: [],     // 404
    errors: []        // Network errors, timeouts
  };

  const totalTests = ALL_DOMAINS.length * PERPS_PATHS.length;
  let completed = 0;

  console.log(`Testing ${ALL_DOMAINS.length} domains × ${PERPS_PATHS.length} endpoints = ${totalTests} total tests\n`);

  for (const domain of ALL_DOMAINS) {
    console.log(`\n🌐 Testing ${domain}...`);

    for (const endpoint of PERPS_PATHS) {
      const result = await testEndpoint(domain, endpoint);
      completed++;

      const progress = `[${completed}/${totalTests}]`;

      if (result.error) {
        // Skip logging errors unless verbose mode
      } else if (result.ok) {
        console.log(`  ✅ ${progress} ${result.status} ${endpoint.desc}`);
        results.working.push(result);
      } else if (INTERESTING_STATUSES.has(result.status)) {
        console.log(`  ⚠️  ${progress} ${result.status} ${endpoint.desc} - ${endpoint.path}`);
        results.interesting.push(result);
      } else if (result.status === 404) {
        results.notFound.push(result);
      } else {
        results.errors.push(result);
      }

      // Small delay to avoid rate limiting
      await new Promise(resolve => setTimeout(resolve, 50));
    }
  }

  // SUMMARY
  console.log('\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('📊 RESULTS SUMMARY');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');

  if (results.working.length > 0) {
    console.log(`\n✅ WORKING ENDPOINTS (${results.working.length}):`);
    console.log('─────────────────────────────────────────────────────────\n');
    for (const r of results.working) {
      console.log(`\n🔗 ${r.url}`);
      console.log(`   Status: ${r.status} ${r.ok ? 'OK' : ''}`);
      console.log(`   Content-Type: ${r.contentType}`);
      console.log(`   Response: ${r.bodyPreview}`);
    }
  }

  if (results.interesting.length > 0) {
    console.log(`\n\n⚠️  INTERESTING ENDPOINTS (${results.interesting.length}):`);
    console.log('─────────────────────────────────────────────────────────');
    console.log('These endpoints exist but reject requests - may need auth/params\n');

    for (const r of results.interesting) {
      console.log(`\n🔗 ${r.url}`);
      console.log(`   Status: ${r.status}`);
      console.log(`   Method: ${r.method}`);
      console.log(`   Response: ${r.bodyPreview}`);

      if (r.status === 400) {
        console.log('   💡 400 = Bad Request - endpoint exists but needs correct parameters');
      } else if (r.status === 401) {
        console.log('   💡 401 = Unauthorized - endpoint exists but needs authentication');
      } else if (r.status === 403) {
        console.log('   💡 403 = Forbidden - endpoint exists but access denied');
      } else if (r.status === 429) {
        console.log('   💡 429 = Rate Limited - endpoint exists but too many requests');
      }
    }
  }

  console.log(`\n\n📈 STATISTICS:`);
  console.log(`─────────────────────────────────────────────────────────`);
  console.log(`✅ Working:     ${results.working.length}`);
  console.log(`⚠️  Interesting: ${results.interesting.length} (endpoint exists, needs auth/params)`);
  console.log(`❌ Not Found:   ${results.notFound.length}`);
  console.log(`🔥 Errors:      ${results.errors.length}`);
  console.log(`📊 Total Tests: ${totalTests}\n`);

  // RECOMMENDATIONS
  console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('💡 RECOMMENDATIONS FOR CLI CONFIG');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n');

  if (results.working.length > 0 || results.interesting.length > 0) {
    const domains = new Set();

    [...results.working, ...results.interesting].forEach(r => {
      domains.add(r.domain);
    });

    console.log('Add these domains to cli_trader.js FALLBACK_PERPS_BASES:\n');
    for (const domain of domains) {
      const examples = [...results.working, ...results.interesting]
        .filter(r => r.domain === domain)
        .slice(0, 3)
        .map(r => `${r.method} ${r.path} (${r.status})`)
        .join(', ');

      console.log(`  "https://${domain}",  // ${examples}`);
    }
  } else {
    console.log('⚠️  No working or interesting endpoints found.');
    console.log('Jupiter Perps may require on-chain RPC queries instead of REST API.');
    console.log('See: github.com/julianfssen/jupiter-perps-anchor-idl-parsing\n');
  }

  console.log('\n✅ Scan complete!\n');
}

main().catch(error => {
  console.error('\n❌ Scanner error:', error.message);
  process.exit(1);
});
