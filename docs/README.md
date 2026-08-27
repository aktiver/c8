# c8 — Next Generation RDF DB

Launch-ready Jekyll source for GitHub Pages. The package contains the complete
Release 1.0 marketing site, deployment book, Helm reference, contributor book,
and feature-first code/function catalog.

## Launch on GitHub Pages now

### Option A — GitHub website

1. Create a new GitHub repository.
2. Extract this ZIP and upload **everything inside the extracted folder** to the repository root, including the hidden `.github` folder.
3. Open **Settings → Pages**.
4. Under **Build and deployment → Source**, select **GitHub Actions**.
5. Open **Actions** and run **Deploy c8 Jekyll site to GitHub Pages**, or push one small change to `main`.

GitHub will show the live address when deployment finishes.

### Option B — command line

```bash
unzip c8-next-generation-jekyll-pages.zip
cd c8-next-generation-jekyll-pages

git init
git add .
git commit -m "Launch c8 Release 1.0 site"
git branch -M main
git remote add origin https://github.com/YOUR-USER/YOUR-REPOSITORY.git
git push -u origin main
```

Then select **GitHub Actions** in **Settings → Pages**.

## URLs

- User or organization site repository named `YOUR-USER.github.io`:
  `https://YOUR-USER.github.io/`
- Ordinary project repository:
  `https://YOUR-USER.github.io/YOUR-REPOSITORY/`

All internal routes and assets use Jekyll's `relative_url` filter, so both
URL forms work without editing `_config.yml`.

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
| `.github/workflows/pages.yml` | Automatic Jekyll build and GitHub Pages deployment |

## Edit the content

Each page is ordinary HTML with Jekyll front matter. Edit it directly, commit,
and push. GitHub Actions rebuilds and redeploys the site automatically.

Run the dependency-free package checks locally with:

```bash
node scripts/verify.mjs
```

## Important product-language boundary

The site labels the product line **Release version 1.0** while retaining the
qualification disclosures found in the reviewed source candidate. Full
applicable SPARQL 1.1 conformance, provider qualification, distributed worker
activation, and production multinode evidence remain acceptance gates rather
than completed claims.
