import { defineConfig } from 'astro/config';
import mdx from '@astrojs/mdx';

export default defineConfig({
  site: 'https://yatagai-mm.github.io',
  base: '/Moqtopus',
  output: 'static',
  trailingSlash: 'always',
  integrations: [mdx()],
  markdown: { shikiConfig: { theme: 'github-dark' } },
});
