import { describe, it, expect } from 'vitest';
import fs from 'fs';
import path from 'path';

const planeRules: Array<{ root: string; forbidden: string[] }> = [
  {
    root: path.join(__dirname, '..', 'src', 'exchange'),
    forbidden: ['analytics', 'strategy', 'risk', 'execution', 'research', 'portfolio', 'metrics', 'state', 'replay', 'globalData'],
  },
  {
    root: path.join(__dirname, '..', 'src', 'globalData'),
    forbidden: ['analytics', 'strategy', 'risk', 'execution', 'research', 'portfolio', 'metrics', 'state', 'replay', 'exchange', 'storage'],
  },
  {
    root: path.join(__dirname, '..', 'src', 'analytics'),
    forbidden: ['research', 'storage', 'exchange', 'replay', 'state', 'metrics', 'globalData'],
  },
  {
    root: path.join(__dirname, '..', 'src', 'research'),
    forbidden: ['analytics', 'exchange', 'strategy', 'risk', 'execution', 'portfolio', 'metrics', 'state', 'replay', 'globalData'],
  },
  {
    root: path.join(__dirname, '..', 'src', 'core', 'research'),
    forbidden: ['analytics'],
  },
  {
    root: path.join(__dirname, '..', 'src', 'strategy'),
    forbidden: ['market', 'exchange', 'execution', 'research', 'analytics'],
  },
  {
    root: path.join(__dirname, '..', 'src', 'risk'),
    forbidden: ['market', 'exchange', 'research', 'analytics'],
  },
  {
    root: path.join(__dirname, '..', 'src', 'execution'),
    forbidden: ['market', 'exchange', 'research', 'analytics'],
  },
  {
    root: path.join(__dirname, '..', 'src', 'portfolio'),
    forbidden: ['market', 'exchange', 'research', 'analytics'],
  },
  {
    root: path.join(__dirname, '..', 'src', 'metrics'),
    forbidden: ['exchange', 'strategy', 'risk', 'execution', 'portfolio'],
  },
];

function listFiles(dir: string): string[] {
  if (!fs.existsSync(dir)) return [];
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  const files: string[] = [];
  for (const e of entries) {
    const full = path.join(dir, e.name);
    if (e.isDirectory()) files.push(...listFiles(full));
    else if (e.isFile() && (full.endsWith('.ts') || full.endsWith('.tsx'))) files.push(full);
  }
  return files;
}

function findImports(content: string): string[] {
  const imports: string[] = [];
  const regex = /import[^;]*from\s+['"]([^'"]+)['"]/g;
  let m: RegExpExecArray | null;
  while ((m = regex.exec(content))) {
    imports.push(m[1]);
  }
  return imports;
}

describe('Architecture guard: no direct cross-plane imports', () => {
  it('rejects forbidden imports across planes', () => {
    const violations: Array<{ file: string; target: string }> = [];

    for (const rule of planeRules) {
      const files = listFiles(rule.root);
      for (const file of files) {
        const content = fs.readFileSync(file, 'utf8');
        const imports = findImports(content);
        for (const imp of imports) {
          for (const ban of rule.forbidden) {
            // forbid segments like ../strategy, /strategy, or @/strategy
            if (imp.includes(`/${ban}`) || imp.includes(`../${ban}`) || imp.startsWith(`${ban}/`) || imp === ban) {
              violations.push({ file, target: imp });
            }
          }
        }
      }
    }

    const message = violations
      .map((v) => `${path.relative(path.join(__dirname, '..'), v.file)} imports ${v.target}`)
      .join('\n');
    expect(violations, message || 'No cross-plane imports detected').toHaveLength(0);
  });
});
