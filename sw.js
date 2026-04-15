const CACHE_VERSION = 'v1';
const STATIC_CACHE = `oranim-static-${CACHE_VERSION}`;
const CDN_CACHE = `oranim-cdn-${CACHE_VERSION}`;
const DATA_CACHE = `oranim-data-${CACHE_VERSION}`;

// Core app files to pre-cache on install
const STATIC_ASSETS = [
  './',
  './index.html',
  './manifest.json',
  './icons/favicon.svg',
  './icons/icon-192.png',
  './icons/icon-512.png',
];

// CDN libraries to pre-cache (versioned URLs, rarely change)
const CDN_ASSETS = [
  'https://unpkg.com/leaflet@1.9.4/dist/leaflet.css',
  'https://unpkg.com/leaflet@1.9.4/dist/leaflet.js',
  'https://unpkg.com/react@18/umd/react.production.min.js',
  'https://unpkg.com/react-dom@18/umd/react-dom.production.min.js',
  'https://unpkg.com/@babel/standalone/babel.min.js',
  'https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js',
  'https://cdnjs.cloudflare.com/ajax/libs/dom-to-image/2.6.0/dom-to-image.min.js',
  'https://unpkg.com/leaflet.heat@0.2.0/dist/leaflet-heat.js',
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

  // Strategy 2: Cache-first for Google Fonts
  if (url.hostname === 'fonts.googleapis.com' || url.hostname === 'fonts.gstatic.com') {
    event.respondWith(
      caches.match(event.request).then((cached) => cached || fetchAndCache(event.request, CDN_CACHE))
    );
    return;
  }

  // Strategy 3: Network-first for data files (GeoJSON/JSON updated periodically)
  if (url.pathname.includes('/data/') && (url.pathname.endsWith('.geojson') || url.pathname.endsWith('.json') || url.pathname.endsWith('.js'))) {
    event.respondWith(networkFirst(event.request, DATA_CACHE));
    return;
  }

  // Strategy 4: Network-first for HTML (get updates when online)
  if (event.request.mode === 'navigate' || url.pathname.endsWith('.html')) {
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
