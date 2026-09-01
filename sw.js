const CACHE_VERSION = 'v40-social-appendix2';

// Small, fast-changing data files we want fresh on every reload.
// SWR (Strategy 3) shows yesterday's data until the SECOND refresh —
// these need network-first so an update lands immediately.
const FRESH_DATA_FILES = [
  '/data/meetings.json',
  '/data/last_update.txt',
  '/data/last_run_summary.txt',
  '/data/developer_aliases.json',
  '/data/hafrash_permit_use.json',
];
const STATIC_CACHE = `oranim-static-${CACHE_VERSION}`;
const CDN_CACHE = `oranim-cdn-${CACHE_VERSION}`;
const DATA_CACHE = `oranim-data-${CACHE_VERSION}`;

// Core app files to pre-cache on install
const STATIC_ASSETS = [
  './',
  './index.html',
  './manifest.json',
  './icons/favicon.svg',
  './icons/icon-192-v2.png',
  './icons/icon-512-v2.png',
];

// CDN libraries to pre-cache (versioned URLs, rarely change).
// Keep this list in sync with the <script>/<link> tags in index.html.
const CDN_ASSETS = [
  'https://unpkg.com/leaflet@1.9.4/dist/leaflet.css',
  'https://unpkg.com/leaflet@1.9.4/dist/leaflet.js',
  'https://cdnjs.cloudflare.com/ajax/libs/proj4js/2.11.0/proj4.js',
  'https://unpkg.com/react@18/umd/react.production.min.js',
  'https://unpkg.com/react-dom@18/umd/react-dom.production.min.js',
  'https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js',
  'https://cdnjs.cloudflare.com/ajax/libs/dom-to-image/2.6.0/dom-to-image.min.js',
];

// Install: pre-cache static + CDN assets
self.addEventListener('install', (event) => {
  event.waitUntil(
    Promise.all([
      caches.open(STATIC_CACHE).then((cache) => cache.addAll(STATIC_ASSETS)),
      caches.open(CDN_CACHE).then((cache) => cache.addAll(CDN_ASSETS)),
    ]).then(() => self.skipWaiting())
  );
});

// Activate: clean up old caches
self.addEventListener('activate', (event) => {
  event.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(
        keys
          .filter((key) => key.startsWith('oranim-') && !key.endsWith(CACHE_VERSION))
          .map((key) => caches.delete(key))
      )
    ).then(() => self.clients.claim())
  );
});

// Fetch strategies
self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);

  // Skip non-GET requests
  if (event.request.method !== 'GET') return;

  // Strategy 1: Cache-first for CDN assets (versioned, don't change)
  if (CDN_ASSETS.some((asset) => event.request.url.startsWith(asset.split('?')[0]))) {
    event.respondWith(
      caches.match(event.request).then((cached) => cached || fetchAndCache(event.request, CDN_CACHE))
    );
    return;
  }

  // Strategy 1b: Cache-first for the MapLibre bundle + RTL plugin. They are
  // fetched on demand (not <script> tags), so they are not in CDN_ASSETS and
  // would otherwise hit the network on every load — and the vector basemap is
  // the default, so that is every load.
  if (url.hostname === 'unpkg.com' && /maplibre|mapbox-gl-rtl-text/.test(url.pathname)) {
    event.respondWith(
      caches.match(event.request).then((cached) => cached || fetchAndCache(event.request, CDN_CACHE))
    );
    return;
  }

  // Strategy 2: Cache-first for Google Fonts
  if (url.hostname === 'fonts.googleapis.com' || url.hostname === 'fonts.gstatic.com') {
    event.respondWith(
      caches.match(event.request).then((cached) => cached || fetchAndCache(event.request, CDN_CACHE))
    );
    return;
  }

  // Strategy 3a: Network-first for small, fast-changing data files.
  // These are small enough that hitting the network on every load is fine,
  // and SWR caused 2-refresh lag after data updates.
  if (FRESH_DATA_FILES.some((path) => url.pathname.endsWith(path))) {
    event.respondWith(networkFirst(event.request, DATA_CACHE));
    return;
  }

  // Strategy 3b: Stale-while-revalidate for the rest of /data (big geojsons).
  // Serve from cache instantly (perf!), refresh in background for next visit.
  // GeoJSON data is large; network-first was making repeat visits as slow as
  // first visits. SWR gives instant UI + freshness on the next reload.
  //
  // `versioned: true` keeps the app's ?v=APP_VERSION in the cache KEY. Without it
  // stripCacheBuster() deleted `v` before the lookup, so a cached copy always won
  // and bumping APP_VERSION had NO effect on data for anyone with the SW installed
  // — the documented remedy for stale data simply did not work. Keeping the version
  // in the key makes a bump a guaranteed miss (→ fresh fetch), while same-version
  // repeat visits keep the instant-from-cache behaviour. Superseded versions of the
  // same file are pruned on write so the cache does not grow per release.
  if (url.pathname.includes('/data/') && (url.pathname.endsWith('.geojson') || url.pathname.endsWith('.json') || url.pathname.endsWith('.js'))) {
    event.respondWith(staleWhileRevalidate(event.request, DATA_CACHE, { versioned: true }));
    return;
  }

  // Strategy 4: Network-first for HTML (get updates when online)
  if (event.request.mode === 'navigate' || url.pathname.endsWith('.html')) {
    event.respondWith(networkFirst(event.request, STATIC_CACHE));
    return;
  }

  // Strategy 4b: Network-first for the app bundle. It's versioned via ?v= in
  // index.html, so fetch the exact build when online; networkFirst strips the
  // cache-buster so only ONE copy is kept (no accumulation) and it stays
  // available offline.
  if (url.origin === self.location.origin && url.pathname.endsWith('/app.js')) {
    event.respondWith(networkFirst(event.request, STATIC_CACHE));
    return;
  }

  // Strategy 5: Cache-first for static assets (icons, etc.)
  if (url.origin === self.location.origin) {
    event.respondWith(
      caches.match(event.request).then((cached) => cached || fetchAndCache(event.request, STATIC_CACHE))
    );
    return;
  }

  // Default: skip tile servers and everything else (don't cache map tiles)
});

// Helper: fetch and store in cache
function fetchAndCache(request, cacheName) {
  return fetch(request).then((response) => {
    if (response.ok) {
      const clone = response.clone();
      caches.open(cacheName).then((cache) => cache.put(request, clone));
    }
    return response;
  });
}

// Helper: network-first with cache fallback
function networkFirst(request, cacheName) {
  // Strip cache-buster query params for cache matching
  const cacheRequest = stripCacheBuster(request);

  return fetch(request)
    .then((response) => {
      if (response.ok) {
        const clone = response.clone();
        caches.open(cacheName).then((cache) => cache.put(cacheRequest, clone));
      }
      return response;
    })
    .catch(() => caches.match(cacheRequest).then((cached) => cached || offlineFallback()));
}

// Helper: stale-while-revalidate — cache-first for speed, refresh in background.
// opts.versioned keeps ?v= in the cache key so an APP_VERSION bump is a real miss.
function staleWhileRevalidate(request, cacheName, opts) {
  const versioned = !!(opts && opts.versioned);
  const cacheRequest = versioned ? request : stripCacheBuster(request);
  const fetchPromise = fetch(request)
    .then((response) => {
      if (response.ok) {
        const clone = response.clone();
        caches.open(cacheName).then((cache) =>
          cache.put(cacheRequest, clone).then(() => (versioned ? pruneOtherVersions(cache, request.url) : null))
        );
      }
      return response;
    })
    .catch(() => null);

  return caches.match(cacheRequest).then((cached) => {
    // Serve from cache immediately if available; background fetch updates for next time
    if (cached) return cached;
    // No cache yet — wait for network
    return fetchPromise.then((net) => net || offlineFallback());
  });
}

// Drop cached copies of the same file under a previous ?v=, so keeping the
// version in the cache key costs one copy per file, not one per release.
function pruneOtherVersions(cache, currentUrl) {
  const cur = new URL(currentUrl);
  return cache.keys().then((keys) =>
    Promise.all(
      keys.map((k) => {
        const u = new URL(k.url);
        return u.pathname === cur.pathname && u.search !== cur.search ? cache.delete(k) : null;
      })
    )
  );
}

// Strip ?v=timestamp cache busters used in the app's fetch calls
function stripCacheBuster(request) {
  const url = new URL(request.url);
  url.searchParams.delete('v');
  return new Request(url.toString(), { headers: request.headers });
}

// Offline fallback page
function offlineFallback() {
  return new Response(
    '<html dir="rtl"><body style="font-family:sans-serif;text-align:center;padding:40px;background:#1a1a2e;color:#e0e0e0">' +
    '<h1>אורנים</h1><p>אין חיבור לאינטרנט. נסה שוב מאוחר יותר.</p></body></html>',
    { headers: { 'Content-Type': 'text/html; charset=utf-8' } }
  );
}
