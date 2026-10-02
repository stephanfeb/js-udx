import js from '@eslint/js'
import tseslint from 'typescript-eslint'

export default tseslint.config(
  { ignores: ['**/dist/**', '**/node_modules/**', '.beads/**'] },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    rules: {
      '@typescript-eslint/no-unused-vars': ['error', { argsIgnorePattern: '^_', varsIgnorePattern: '^_' }],
      'no-constant-condition': ['error', { checkLoops: false }]
    }
  },
  {
    // Plain-Node interop scripts run against the built package.
    files: ['tools/**/*.mjs'],
    languageOptions: {
      globals: { process: 'readonly', setTimeout: 'readonly', clearTimeout: 'readonly' }
    }
  }
)
