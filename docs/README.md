# Moqtopus documentation

English API documentation built with [Astro](https://astro.build/) and its
[official MDX integration](https://docs.astro.build/en/guides/integrations-guide/mdx/).
The output is static HTML and CSS, with no client-side JavaScript, UI framework,
external fonts, or server runtime required.

## Development

Requires Node.js 22.12.0 or later (Node.js 24 LTS recommended) and npm.
From this directory:

```sh
npm ci
npm run dev
```

Open the local URL printed by Astro (normally `http://localhost:4321/Moqtopus/`).
To build and inspect the static output:

```sh
npm run build
npm run preview
```

The generated site is in `dist/`. The default configuration targets
`https://yatagai-mm.github.io/Moqtopus/`. Navigation and assets use `/Moqtopus/`;
content links are relative. Use `npm run preview` to serve the build at that base
path locally.

## GitHub Pages

The workflow in `../.github/workflows/docs.yml` installs the locked dependencies,
builds `docs/`, and deploys `docs/dist/` using the official GitHub Pages actions.
No separate publishing branch or personal access token is needed.

One-time repository setup:

1. Open **Settings → Pages → Build and deployment**.
2. Set **Source** to **GitHub Actions**.
3. Push the workflow and documentation to `main`. Alternatively, run
   **Deploy documentation to GitHub Pages** from the **Actions** tab on `main`.

Changes to `docs/**` or the workflow on `main` trigger deployment automatically.
Manual runs from other branches can build, but only `main` can deploy. The
`github-pages` environment may require approval if repository rules enforce it.

During CI, the build uses the origin and base path returned by
`actions/configure-pages`, so GitHub Pages custom-domain settings and forks are
respected. If you change the deployment URL, update `site` and `base` in
`astro.config.mjs` as well to keep local previews consistent.

See GitHub's [custom Pages workflow documentation](https://docs.github.com/en/pages/getting-started-with-github-pages/using-custom-workflows-with-github-pages)
for repository permissions and environment configuration.

## Editing

- Write English MDX pages in `src/pages/`.
- Include `layout: ../layouts/Docs.astro`, `title`, and `description` in frontmatter.
- Add new pages to the navigation in `src/layouts/Docs.astro`.
- Use second-level headings for the automatically generated page outline.
- Shared styling is in `src/styles/docs.css`.

Keep API descriptions synchronized with `../include/moq/` and behavior in
`../src/`. When editing examples, compile complete `cpp` code blocks with a
C++17 compiler and `../include` on its include path. Build the site after editing
to validate MDX and route generation.
