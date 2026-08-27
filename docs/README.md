# c8 — Next Generation RDF DB

Launch-ready Jekyll source for GitHub Pages. The package contains the complete
Release 1.0 marketing site, deployment book, Helm reference, contributor book,
and feature-first code/function catalog.

## Required repository

This package is configured for exactly:

- Repository: `https://github.com/aktiver/c8`
- Published site: `https://aktiver.github.io/c8/`

Upload **everything inside the extracted folder** to the root of the `c8`
repository. This package intentionally contains no custom GitHub Actions
workflow.

## Regular GitHub Pages deployment

1. Open **Settings → Actions → General** in `aktiver/c8`.
2. Make sure GitHub Actions is enabled for the repository. GitHub Pages uses an
   internal deployment workflow even when you choose normal branch publishing;
   you do not create or maintain that workflow.
3. Open **Settings → Pages**.
4. Under **Build and deployment → Source**, select **Deploy from a branch**.
5. Select branch **main**, folder **/(root)**, then click **Save**.
6. GitHub runs its built-in Jekyll build and publishes the site.

Do not select **GitHub Actions** as the Pages source. There is no custom
`.github/workflows/pages.yml` file in this package.

### Upload with the command line

```bash
unzip c8-next-generation-jekyll-pages.zip
cd c8-next-generation-jekyll-pages

git init
git add .
git commit -m "Launch c8 Release 1.0 site"
git branch -M main
git remote add origin https://github.com/aktiver/c8.git
git push -u origin main
```

Then select **Deploy from a branch → main → /(root)** in **Settings → Pages**.

## URL configuration

`_config.yml` is locked to `url: "https://aktiver.github.io"` and
`baseurl: "/c8"`. Internal routes and assets use Jekyll's `relative_url`
filter, so the compiled paths resolve under `/c8`.

## Site structure

| Path | Purpose |
| --- | --- |
| `index.html` | Business value, 8 Cs, standards and architecture |
| `deploy/index.html` | TriG preparation, ingestion and operator runbook |
| `helm/index.html` | RKE2/K3s, EKS, AKS, GKE and on-prem Helm reference |
| `contribute/index.html` | Engineering architecture and contribution guide |
| `catalog/index.html` | Alphabetical file and function catalog |
| `assets/css/site.css` | Compiled c8 visual system plus static-site fixes |
| `assets/js/site.js` | Mobile documentation navigation only |

## Edit the content

Each page is ordinary HTML with Jekyll front matter. Edit it directly, commit,
and push. GitHub Pages rebuilds the selected branch automatically.

Run the dependency-free package checks locally with:

```bash
node scripts/verify.mjs
```

Run the same Jekyll compile and output audit used for this release with:

```bash
JEKYLL_ENV=production jekyll build --trace
node scripts/verify.mjs --built
```

## Important product-language boundary

The site labels the product line **Release version 1.0** while retaining the
qualification disclosures found in the reviewed source candidate. Full
applicable SPARQL 1.1 conformance, provider qualification, distributed worker
activation, and production multinode evidence remain acceptance gates rather
than completed claims.
