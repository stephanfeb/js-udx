import { fileURLToPath } from 'node:url'
import { defineConfig } from 'vitest/config'

const here = (path: string): string => fileURLToPath(new URL(path, import.meta.url))

// Tests run against workspace sources, so `npm test` doesn't need a build first.
const udxSrc = here('./packages/udx/src/index.ts')

export default defineConfig({
  test: {
    projects: [
      {
        test: {
          name: 'udx',
          root: here('./packages/udx'),
          include: ['test/**/*.test.ts']
        }
      },
      {
        resolve: {
          alias: { '@stephanfeb/udx': udxSrc }
        },
        test: {
          name: 'libp2p-udx',
          root: here('./packages/libp2p-udx'),
          include: ['test/**/*.test.ts']
        }
      }
    ]
  }
})
