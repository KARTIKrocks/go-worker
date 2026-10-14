import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import type { ReactNode } from 'react';

import styles from './index.module.css';

type Feature = {
  readonly title: string;
  readonly description: string;
};

type Capability = {
  readonly capability: string;
};

type Stat = {
  readonly label: string;
  readonly value: string;
  readonly unit: string;
  readonly context: string;
};

const FEATURES = [
  {
    title: 'Bounded Worker Pool',
    description:
      'A fixed set of goroutines and a bounded queue, with blocking, non-blocking, and context-aware submits',
  },
  {
    title: 'Retries with Backoff',
    description:
      'Exponential backoff, a delay cap, jitter, per-attempt timeouts, and permanent errors that skip retrying',
  },
  {
    title: 'Panic Recovery',
    description:
      'A panicking job returns a PanicError with its stack trace; the worker keeps running',
  },
  {
    title: 'Graceful Shutdown',
    description:
      'Drain the queue on Close, or force-cancel in-flight jobs after a timeout',
  },
  {
    title: 'Typed Futures',
    description:
      'Submit a func returning (T, error) and await a typed result, with generics',
  },
  {
    title: 'Groups and Batches',
    description:
      'Fan out with error collection or cancel-on-first-error, and process slices in chunks',
  },
  {
    title: 'Scheduler',
    description:
      'Cron expressions, intervals, daily times, and one-shot tasks with overlap policies and jitter',
  },
  {
    title: 'Flow Control',
    description:
      'Ticker, debouncer, throttler, and token-bucket rate limiter, all on top of the pool',
  },
  {
    title: 'Middleware and Hooks',
    description:
      'Wrap every attempt for tracing or logging, and observe job and worker lifecycles',
  },
  {
    title: 'Metrics',
    description:
      'Atomic counters for submitted, completed, failed, retried, and panicked jobs, plus throughput',
  },
  {
    title: 'Pause and Resume',
    description:
      'Stop picking up new work without shutting down, then carry on where you left off',
  },
  {
    title: 'Zero Dependencies',
    description: 'Standard library only — nothing to audit, nothing to update',
  },
] as const satisfies readonly Feature[];

// Everything go-worker provides that a hand-rolled `go func()` plus a
// WaitGroup leaves to you.
const CAPABILITIES = [
  { capability: 'Bounded concurrency and backpressure' },
  { capability: 'Retries with backoff, jitter, and permanent errors' },
  { capability: 'Panic recovery with stack traces' },
  { capability: 'Per-attempt timeouts and cancellation' },
  { capability: 'Graceful drain and forced shutdown' },
  { capability: 'Cron, interval, and one-shot scheduling' },
  { capability: 'Debounce, throttle, and rate limiting' },
  { capability: 'Metrics and lifecycle hooks' },
] as const satisfies readonly Capability[];

// Measured on an Intel i5-11400H @ 2.70GHz (12 cores), Go 1.27, Linux, with
// `go test -bench=. -benchmem` in the repository root: an 8-worker pool with
// 12 goroutines submitting no-op jobs. Submit runs with the queue full, so its
// per-op cost is the pool's steady-state throughput.
const STATS = [
  {
    label: 'Submit',
    value: '0.9',
    unit: 'µs/op',
    context: '5 allocs, sustained with a full queue',
  },
  {
    label: 'SubmitWait',
    value: '1.5',
    unit: 'µs/op',
    context: '8 allocs, including the result handoff',
  },
  {
    label: 'Throughput',
    value: '~1.1M',
    unit: 'jobs/s',
    context: 'no-op jobs, 8 workers',
  },
  {
    label: 'Dependencies',
    value: '0',
    unit: '',
    context: 'standard library only',
  },
] as const satisfies readonly Stat[];

const INSTALL_COMMAND = 'go get github.com/KARTIKrocks/go-worker';

const REPO_URL = 'https://github.com/KARTIKrocks/go-worker';

function Hero(): ReactNode {
  return (
    <header className={styles.hero}>
      <div className="container">
        <h1 className={styles.title}>Zero-dependency worker pool for Go</h1>
        <p className={styles.subtitle}>
          A bounded worker pool with retries, panic recovery, and graceful
          shutdown — plus typed futures, error groups, batching, a cron
          scheduler, and flow-control helpers. Standard library only.
        </p>

        <div className={styles.buttons}>
          <Link
            className="button button--primary button--lg"
            to="/docs/getting-started">
            Get Started
          </Link>
          <Link
            className="button button--secondary button--lg"
            to="https://pkg.go.dev/github.com/KARTIKrocks/go-worker">
            API Reference
          </Link>
        </div>

        <div className={styles.install}>
          <span className={styles.prompt} aria-hidden="true">
            $
          </span>
          <code>{INSTALL_COMMAND}</code>
        </div>
      </div>
    </header>
  );
}

function Features(): ReactNode {
  return (
    <section className="container" aria-label="Features">
      <div className={styles.features}>
        {FEATURES.map((feature) => (
          <article key={feature.title} className={styles.card}>
            <h2>{feature.title}</h2>
            <p>{feature.description}</p>
          </article>
        ))}
      </div>
    </section>
  );
}

function WhyGoWorker(): ReactNode {
  return (
    <section className={styles.section}>
      <div className="container">
        <h2 className={styles.sectionTitle}>Why go-worker?</h2>
        <p className={styles.sectionLead}>
          A <code>go</code> statement and a <code>sync.WaitGroup</code> get you
          concurrency. Running background work safely in production — bounded,
          retried, cancellable, observable, and shut down cleanly — is the part
          go-worker provides.
        </p>

        <div className={styles.tableScroll}>
          <table className={styles.compare}>
            <thead>
              <tr>
                <th scope="col">Capability</th>
                <th scope="col">go-worker</th>
                <th scope="col">Goroutines + WaitGroup</th>
              </tr>
            </thead>
            <tbody>
              {CAPABILITIES.map(({ capability }) => (
                <tr key={capability}>
                  <th scope="row">{capability}</th>
                  <td>
                    <span className={styles.check} aria-hidden="true">
                      ✓
                    </span>
                    <span className={styles.srOnly}>Included</span>
                  </td>
                  <td className={styles.diy}>You build it</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </section>
  );
}

function Performance(): ReactNode {
  return (
    <section className={styles.section}>
      <div className="container">
        <h2 className={styles.sectionTitle}>Performance</h2>
        <p className={styles.sectionLead}>
          Measured on an Intel i5-11400H (12 cores), Go 1.27, Linux, with an
          8-worker pool and 12 goroutines submitting no-op jobs concurrently —
          so these figures are the pool's own overhead, not your jobs'.
        </p>

        <div className={styles.stats}>
          {STATS.map((stat) => (
            <article key={stat.label} className={styles.stat}>
              <h3 className={styles.statLabel}>{stat.label}</h3>
              <p className={styles.statValue}>
                {stat.value}
                {stat.unit && (
                  <span className={styles.statUnit}> {stat.unit}</span>
                )}
              </p>
              <p className={styles.statContext}>{stat.context}</p>
            </article>
          ))}
        </div>

        <p className={styles.sectionNote}>
          Reproduce them yourself with{' '}
          <code>go test -bench=. -benchmem ./...</code> in the repository root.
          The <Link to={`${REPO_URL}#benchmarks`}>benchmark table</Link> in the
          README lists every figure measured.
        </p>
      </div>
    </section>
  );
}

export default function Home(): ReactNode {
  const { siteConfig } = useDocusaurusContext();

  return (
    <Layout
      title={siteConfig.tagline}
      description="A high-performance, zero-dependency worker pool for Go with retries, panic recovery, graceful shutdown, typed futures, error groups, batching, and a cron scheduler.">
      <Hero />
      <main>
        <Features />
        <WhyGoWorker />
        <Performance />
      </main>
    </Layout>
  );
}
