import { readFile, stat } from "node:fs/promises";

const pages = [
  ["index.html", "/"],
  ["deploy/index.html", "/deploy/"],
  ["helm/index.html", "/helm/"],
  ["contribute/index.html", "/contribute/"],
  ["catalog/index.html", "/catalog/"],
  ["404.html", "/404.html"],
];

const failures = [];
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

const workflow = await readFile(".github/workflows/pages.yml", "utf8");
for (const action of ["actions/configure-pages@v5", "actions/jekyll-build-pages@v1", "actions/deploy-pages@v5"]) {
  if (!workflow.includes(action)) failures.push("workflow is missing " + action);
}

if (failures.length) {
  console.error(failures.join("\n"));
  process.exit(1);
}
console.log("Verified " + pages.length + " Jekyll pages, base-path-safe links, assets, and GitHub Pages workflow.");
