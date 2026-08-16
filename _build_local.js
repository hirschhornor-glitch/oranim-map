// Local Babel build (no npm). Uses Antigravity's bundled @babel/core.
// Run with Playwright's node.exe:  node.exe _build_local.js
const fs = require('fs');
const path = require('path');
const nm = process.env.BABEL_NM;
const babel = require(path.join(nm, '@babel', 'core'));
const presetReact = path.join(nm, '@babel', 'preset-react');
const src = fs.readFileSync('src/app.jsx', 'utf8');
const out = babel.transformSync(src, {
  presets: [presetReact],
  babelrc: false,
  configFile: false,
  compact: false,
});
fs.writeFileSync('app.js', out.code);
console.log('wrote app.js', out.code.length, 'bytes');
