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

Open the local URL printed by Astro. To build and inspect the static output:

```sh
npm run build
npm run preview
```

The generated site is in `dist/`. Serve that directory with any static host.
For hosting under a subpath, set `base` in `astro.config.mjs` (for example,
`base: '/Moqtopus'`) before building. Navigation respects Astro's base path;
content links are relative. No hosting service is configured by this project.

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
