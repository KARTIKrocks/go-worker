import type { SidebarsConfig } from '@docusaurus/plugin-content-docs';

// This runs in Node.js - Don't use client-side code here (browser APIs, JSX...)

/**
 * The pool comes first because everything else runs on one: configuring and
 * operating it, then extending it, then the higher-level patterns built on
 * top (async results, scheduling, flow control), then operational concerns.
 */
const sidebars: SidebarsConfig = {
  docsSidebar: [
    'intro',
    'getting-started',
    {
      type: 'category',
      label: 'Pool',
      collapsed: false,
      items: ['pool', 'configuration', 'retries', 'lifecycle'],
    },
    {
      type: 'category',
      label: 'Extending',
      collapsed: false,
      items: ['middleware', 'hooks'],
    },
    {
      type: 'category',
      label: 'Concurrency Patterns',
      collapsed: false,
      items: ['future', 'groups', 'batch'],
    },
    {
      type: 'category',
      label: 'Scheduling',
      collapsed: false,
      items: ['scheduler', 'schedules'],
    },
    {
      type: 'category',
      label: 'Flow Control',
      collapsed: false,
      items: ['ticker', 'debouncer', 'throttler', 'rate-limiter'],
    },
    {
      type: 'category',
      label: 'Operations',
      collapsed: false,
      items: ['metrics', 'errors'],
    },
  ],
};

export default sidebars;
