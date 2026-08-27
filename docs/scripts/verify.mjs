import { access, readFile, stat } from "node:fs/promises";
import path from "node:path";

const pages = [
  ["index.html", "/"],
  ["deploy/index.html", "/deploy/"],
  ["helm/index.html", "/helm/"],
  ["contribute/index.html", "/contribute/"],
  ["catalog/index.html", "/catalog/"],
  ["404.html", "/404.html"],
];

const failures = [];
const config = await readFile("_config.yml", "utf8");
for (const required of [
  'url: "https://aktiver.github.io"',
  'baseurl: "/c8"',
  "safe: true",
  "strict_front_matter: true",
]) {
  if (!config.includes(required)) failures.push("_config.yml is missing " + required);
}

for (const [file, permalink] of pages) {
  const html = await readFile(file, "utf8");
  if (!html.startsWith("---\n")) failures.push(file + ": missing Jekyll front matter");
  if (!html.includes("permalink: " + permalink)) failures.push(file + ": wrong permalink");
  if (!html.includes("<!DOCTYPE html>")) failures.push(file + ": missing HTML document");
  if (!html.includes("| relative_url")) failures.push(file + ": missing base-path-safe URLs");
  if (/(?:_vinext|_next|type=["']module["'])/.test(html)) failures.push(file + ": contains framework runtime output");
  if (/href=["']\/(?:deploy|helm|contribute|catalog)?(?:[#"'])/.test(html)) failures.push(file + ": contains a root-hardcoded internal link");
}

const css = await stat("assets/css/site.css");
const hero = await stat("assets/images/c8-warp-data-ribbon-hero.png");
if (css.size < 100000) failures.push("site.css is unexpectedly small");
if (hero.size < 1000000) failures.push("hero image is unexpectedly small");

try {
  await access(".github/workflows/pages.yml");
  failures.push("custom Pages workflow must be absent for branch deployment");
} catch {
  // Expected: GitHub Pages owns the regular branch-deployment workflow.
}

if (process.argv.includes("--built")) {
  const builtPages = [
    ["_site/index.html", "/"],
    ["_site/deploy/index.html", "/deploy/"],
    ["_site/helm/index.html", "/helm/"],
    ["_site/contribute/index.html", "/contribute/"],
    ["_site/catalog/index.html", "/catalog/"],
    ["_site/404.html", "/404.html"],
  ];

  const routeFile = (pathname) => {
    const clean = pathname.replace(/^\/c8\/?/, "");
    if (!clean) return "_site/index.html";
    if (clean === "404.html") return "_site/404.html";
    return path.join("_site", clean, "index.html");
  };

  for (const [file, route] of builtPages) {
    let html;
    try {
      html = await readFile(file, "utf8");
    } catch {
      failures.push(file + ": missing compiled page for " + route);
      continue;
    }

    if (/\{[{%]|[}%]\}/.test(html)) failures.push(file + ": unresolved Liquid markup");
    if (/(?:_vinext|_next|type=["']module["'])/.test(html)) failures.push(file + ": framework runtime leaked into output");

    const ids = [...html.matchAll(/\sid=["']([^"']+)["']/g)].map((match) => match[1]);
    const duplicates = ids.filter((id, index) => ids.indexOf(id) !== index);
    if (duplicates.length) failures.push(file + ": duplicate ids: " + [...new Set(duplicates)].join(", "));

    const references = [...html.matchAll(/\s(?:href|src)=["']([^"']+)["']/g)].map((match) => match[1]);
    for (const reference of references) {
      if (/^(?:https?:|mailto:|tel:|data:|javascript:)/.test(reference)) continue;
      if (reference.startsWith("#")) {
        if (reference.length > 1 && !ids.includes(decodeURIComponent(reference.slice(1)))) {
          failures.push(file + ": missing local anchor " + reference);
        }
        continue;
      }
      if (!reference.startsWith("/c8/")) {
        failures.push(file + ": path escaped /c8 baseurl: " + reference);
        continue;
      }

      const parsed = new URL(reference, "https://aktiver.github.io");
      if (parsed.pathname.startsWith("/c8/assets/")) {
        const asset = path.join("_site", parsed.pathname.slice("/c8/".length));
        try {
          await access(asset);
        } catch {
          failures.push(file + ": missing asset " + reference);
        }
        continue;
      }

      const target = routeFile(parsed.pathname);
      try {
        const targetHtml = await readFile(target, "utf8");
        if (parsed.hash) {
          const targetId = decodeURIComponent(parsed.hash.slice(1));
          const escaped = targetId.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
          if (!new RegExp(`\\sid=["']${escaped}["']`).test(targetHtml)) {
            failures.push(file + ": missing target anchor " + reference);
          }
        }
      } catch {
        failures.push(file + ": missing internal route " + reference);
      }
    }
  }

  for (const asset of [
    "_site/assets/css/site.css",
    "_site/assets/js/site.js",
    "_site/assets/images/favicon.svg",
    "_site/assets/images/c8-warp-data-ribbon-hero.png",
  ]) {
    try {
      await access(asset);
    } catch {
      failures.push(asset + ": missing compiled asset");
    }
  }
}

if (failures.length) {
  console.error(failures.join("\n"));
  process.exit(1);
}
console.log(
  "Verified " + pages.length + " Jekyll pages, the /c8 base path, assets, branch deployment, " +
  (process.argv.includes("--built") ? "and compiled output." : "and source contracts."),
);
