# c8 — GitHub Pages package

This archive is prepared for the existing repository and Pages source:

- Repository: `https://github.com/aktiver/c8`
- Published URL: `https://aktiver.github.io/c8/`
- Pages source: `main` branch, `/docs` folder
- Deployment mode: **Deploy from a branch**

## Upload

Extract the ZIP, open `https://github.com/aktiver/c8`, choose **Add file →
Upload files**, and upload the included `docs` folder to the repository root.
The final repository paths must be:

```text
docs/_config.yml
docs/index.html
docs/404.html
docs/assets/
docs/deploy/index.html
docs/helm/index.html
docs/contribute/index.html
docs/catalog/index.html
```

Commit the upload directly to `main`. Then open **Settings → Pages** and select
**Deploy from a branch → main → /docs**.

This package intentionally contains no `.github/workflows` directory and no
custom deployment workflow. GitHub still requires Actions to be available for
its internal Pages deployment, even when **Deploy from a branch** is selected.

## URL configuration

`docs/_config.yml` uses:

```yaml
url: "https://aktiver.github.io"
baseurl: "/c8"
repository: "aktiver/c8"
```

Every local route and asset uses Jekyll's `relative_url` filter, so generated
links stay under `/c8`.

## Verification

From the extracted package:

```bash
cd docs
node scripts/verify.mjs
```

The verifier checks the six Jekyll pages, front matter, `/c8` routing, assets,
HTML structure, internal anchors, and the absence of a custom Pages workflow.
