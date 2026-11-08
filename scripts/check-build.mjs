import { spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';

const localPython = process.platform === 'win32'
  ? '.venv/Scripts/python.exe'
  : '.venv/bin/python';
const python = existsSync(localPython)
  ? localPython
  : process.platform === 'win32' ? 'py' : 'python3';
const prefix = python === 'py' ? ['-3'] : [];

for (const args of [
  ['-m', 'compileall', '-q', 'main.py', 'auth_service.py', 'database.py', 'models.py', 'stream_consumer.py'],
  ['-m', 'pip', 'check'],
]) {
  const result = spawnSync(python, [...prefix, ...args], { stdio: 'inherit' });
  if (result.error) {
    console.error(result.error.message);
    process.exit(1);
  }
  if (result.status !== 0) process.exit(result.status ?? 1);
}
