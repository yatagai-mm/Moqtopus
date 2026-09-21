import { defineConfig } from 'astro/config';
import mdx from '@astrojs/mdx';

export default defineConfig({
  output: 'static',
  trailingSlash: 'always',
  integrations: [mdx()],
  markdown: { shikiConfig: { theme: 'github-dark' } },
});
