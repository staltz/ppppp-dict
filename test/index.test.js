const test = require('tape')
const path = require('path')
const os = require('os')
const rimraf = require('rimraf')
const SecretStack = require('secret-stack')
const FeedV1 = require('ppppp-db/feed-v1')
const caps = require('ssb-caps')
const p = require('util').promisify
const { generateKeypair } = require('./util')

const DIR = path.join(os.tmpdir(), 'ppppp-record')
rimraf.sync(DIR)

const aliceKeys = generateKeypair('alice')
const who = aliceKeys.id

let peer
test('setup', async (t) => {
  peer = SecretStack({ appKey: caps.shs })
    .use(require('ppppp-db'))
    .use(require('ssb-box'))
    .use(require('../lib'))
    .call(null, {
      keys: aliceKeys,
      path: DIR,
    })

  await peer.db.loaded()
})

test('Record update() and get()', async (t) => {
  t.ok(
    await p(peer.record.update)(who, 'profile', { name: 'alice' }),
    'update .name'
  )
  t.deepEqual(peer.record.get(who, 'profile'), { name: 'alice' }, 'get')

  const fieldRoots1 = peer.record._getFieldRoots('profile')
  t.deepEquals(fieldRoots1, { name: ['Pt4YwxksvCLir45Tmw3hXK'] }, 'fieldRoots')

  t.ok(await p(peer.record.update)(who, 'profile', { age: 20 }), 'update .age')
  t.deepEqual(
    peer.record.get(who, 'profile'),
    { name: 'alice', age: 20 },
    'get'
  )

  const fieldRoots2 = peer.record._getFieldRoots('profile')
  t.deepEquals(
    fieldRoots2,
    { name: ['Pt4YwxksvCLir45Tmw3hXK'], age: ['XqkG9Uz1eQcxv9R1f3jgKS'] },
    'fieldRoots'
  )

  t.false(
    await p(peer.record.update)(who, 'profile', { name: 'alice' }),
    'redundant update .name'
  )
  t.deepEqual(
    peer.record.get(who, 'profile'),
    { name: 'alice', age: 20 },
    'get'
  )

  t.true(
    await p(peer.record.update)(who, 'profile', { name: 'Alice' }),
    'update .name'
  )
  t.deepEqual(
    peer.record.get(who, 'profile'),
    { name: 'Alice', age: 20 },
    'get'
  )

  const fieldRoots3 = peer.record._getFieldRoots('profile')
  t.deepEquals(
    fieldRoots3,
    { name: ['WGDGt1UEGPpRyutfDyC2we'], age: ['XqkG9Uz1eQcxv9R1f3jgKS'] },
    'fieldRoots'
  )
})

test('Record squeeze', async (t) => {
  t.ok(await p(peer.record.update)(who, 'profile', { age: 21 }), 'update .age')
  t.ok(await p(peer.record.update)(who, 'profile', { age: 22 }), 'update .age')
  t.ok(await p(peer.record.update)(who, 'profile', { age: 23 }), 'update .age')

  const fieldRoots4 = peer.record._getFieldRoots('profile')
  t.deepEquals(
    fieldRoots4,
    { name: ['WGDGt1UEGPpRyutfDyC2we'], age: ['6qu5mbLbFPJHCFge7QtU48'] },
    'fieldRoots'
  )

  t.equals(peer.record._squeezePotential('profile'), 3, 'squeezePotential=3')
  t.true(await p(peer.record.squeeze)(who, 'profile'), 'squeezed')

  const fieldRoots5 = peer.record._getFieldRoots('profile')
  t.deepEquals(
    fieldRoots5,
    { name: ['Ba96TjutuuPbdMMvNS4BbL'], age: ['Ba96TjutuuPbdMMvNS4BbL'] },
    'fieldRoots'
  )

  t.equals(peer.record._squeezePotential('profile'), 0, 'squeezePotential=0')
  t.false(await p(peer.record.squeeze)(who, 'profile'), 'squeeze idempotent')

  const fieldRoots6 = peer.record._getFieldRoots('profile')
  t.deepEquals(fieldRoots6, fieldRoots5, 'fieldRoots')
})

test('Record receives old branched update', async (t) => {
  const rootMsg = FeedV1.createRoot(aliceKeys, 'record_v1__profile')
  const rootHash = FeedV1.getMsgHash(rootMsg)

  const tangle = new FeedV1.Tangle(rootHash)
  tangle.add(rootHash, rootMsg)

  const msg = FeedV1.create({
    keys: aliceKeys,
    type: 'record_v1__profile',
    content: { update: { age: 2 }, supersedes: [] },
    tangles: {
      [rootHash]: tangle,
    },
  })
  const rec = await p(peer.db.add)(msg, rootHash)
  t.equals(rec.hash, 'JXvFSXE9s1DF77cSu5XUm', 'msg hash')

  const fieldRoots7 = peer.record._getFieldRoots('profile')
  t.deepEquals(
    fieldRoots7,
    {
      name: ['Ba96TjutuuPbdMMvNS4BbL'],
      age: ['Ba96TjutuuPbdMMvNS4BbL', rec.hash],
    },
    'fieldRoots'
  )

  t.equals(peer.record._squeezePotential('profile'), 6, 'squeezePotential=6')
})

test('teardown', (t) => {
  peer.close(true, t.end)
})
