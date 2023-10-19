const test = require('node:test')
const assert = require('node:assert')
const path = require('path')
const os = require('os')
const rimraf = require('rimraf')
const MsgV3 = require('ppppp-db/msg-v3')
const Keypair = require('ppppp-keypair')
const p = require('util').promisify
const { createPeer } = require('./util')

const DIR = path.join(os.tmpdir(), 'ppppp-record')
rimraf.sync(DIR)

const aliceKeypair = Keypair.generate('ed25519', 'alice')

let peer
let aliceID
test('setup', async (t) => {
  peer = createPeer({
    keypair: aliceKeypair,
    path: DIR,
    record: { ghostSpan: 4 },
  })

  await peer.db.loaded()

  aliceID = await p(peer.db.account.create)({
    domain: 'account',
    _nonce: 'alice',
  })
  await p(peer.record.load)(aliceID)
})

test('Record update() and get()', async (t) => {
  assert(
    await p(peer.record.update)('profile', { name: 'alice' }),
    'update .name'
  )
  assert.deepEqual(
    peer.record.read(aliceID, 'profile'),
    { name: 'alice' },
    'get'
  )

  const fieldRoots1 = peer.record.getFieldRoots('profile')
  assert.deepEqual(
    fieldRoots1,
    { name: ['PbwnLbJS4oninQ1RPCdgRn'] },
    'fieldRoots'
  )

  assert(await p(peer.record.update)('profile', { age: 20 }), 'update .age')
  assert.deepEqual(
    peer.record.read(aliceID, 'profile'),
    { name: 'alice', age: 20 },
    'get'
  )

  const fieldRoots2 = peer.record.getFieldRoots('profile')
  assert.deepEqual(
    fieldRoots2,
    { name: ['PbwnLbJS4oninQ1RPCdgRn'], age: ['9iTTqNabtnXmw4AiZxNMRq'] },
    'fieldRoots'
  )

  assert.equal(
    await p(peer.record.update)('profile', { name: 'alice' }),
    false,
    'redundant update .name'
  )
  assert.deepEqual(
    peer.record.read(aliceID, 'profile'),
    { name: 'alice', age: 20 },
    'get'
  )

  assert.equal(
    await p(peer.record.update)('profile', { name: 'Alice' }),
    true,
    'update .name'
  )
  assert.deepEqual(
    peer.record.read(aliceID, 'profile'),
    { name: 'Alice', age: 20 },
    'get'
  )

  const fieldRoots3 = peer.record.getFieldRoots('profile')
  assert.deepEqual(
    fieldRoots3,
    { age: ['9iTTqNabtnXmw4AiZxNMRq'], name: ['M2JhM7TE2KX5T5rfnxBh6M'] },
    'fieldRoots'
  )
})

test('Record squeeze', async (t) => {
  assert(await p(peer.record.update)('profile', { age: 21 }), 'update .age')
  assert(await p(peer.record.update)('profile', { age: 22 }), 'update .age')
  assert(await p(peer.record.update)('profile', { age: 23 }), 'update .age')

  const fieldRoots4 = peer.record.getFieldRoots('profile')
  assert.deepEqual(
    fieldRoots4,
    { name: ['M2JhM7TE2KX5T5rfnxBh6M'], age: ['S3xiydrT6Y34Bp1vg6wN7P'] },
    'fieldRoots'
  )

  assert.equal(
    peer.record._squeezePotential('profile'),
    3,
    'squeezePotential=3'
  )
  assert.equal(await p(peer.record.squeeze)('profile'), true, 'squeezed')

  const fieldRoots5 = peer.record.getFieldRoots('profile')
  assert.deepEqual(
    fieldRoots5,
    { name: ['Y4JkpPCHN8Avtz4VALaAmK'], age: ['Y4JkpPCHN8Avtz4VALaAmK'] },
    'fieldRoots'
  )

  assert.equal(
    peer.record._squeezePotential('profile'),
    0,
    'squeezePotential=0'
  )
  assert.equal(
    await p(peer.record.squeeze)('profile'),
    false,
    'squeeze idempotent'
  )

  const fieldRoots6 = peer.record.getFieldRoots('profile')
  assert.deepEqual(fieldRoots6, fieldRoots5, 'fieldRoots')
})

test('Record isRoot', (t) => {
  const moot = MsgV3.createMoot(aliceID, 'record_v1__profile', aliceKeypair)
  assert.ok(peer.record.isRoot(moot), 'isRoot')
})

test('Record isGhostable', (t) => {
  const moot = MsgV3.createMoot(aliceID, 'record_v1__profile', aliceKeypair)
  const mootID = MsgV3.getMsgID(moot)

  const tangle = peer.db.getTangle(mootID)
  const msgIDs = tangle.topoSort()

  const fieldRoots = peer.record.getFieldRoots('profile')
  assert.deepEqual(fieldRoots.age, [msgIDs[7]])

  // Remember from the setup, that ghostSpan=4
  assert.equal(msgIDs.length, 8);
  assert.equal(peer.record.isGhostable(msgIDs[0], mootID), false) // moot
  assert.equal(peer.record.isGhostable(msgIDs[1], mootID), false)
  assert.equal(peer.record.isGhostable(msgIDs[2], mootID), false)
  assert.equal(peer.record.isGhostable(msgIDs[3], mootID), true) // in ghostSpan
  assert.equal(peer.record.isGhostable(msgIDs[4], mootID), true) // in ghostSpan
  assert.equal(peer.record.isGhostable(msgIDs[5], mootID), true) // in ghostSpan
  assert.equal(peer.record.isGhostable(msgIDs[6], mootID), true) // in ghostSpan
  assert.equal(peer.record.isGhostable(msgIDs[7], mootID), false) // field root
})

test('Record receives old branched update', async (t) => {
  const moot = MsgV3.createMoot(aliceID, 'record_v1__profile', aliceKeypair)
  const mootID = MsgV3.getMsgID(moot)

  assert.equal(
    peer.record.getMinRequiredDepth(mootID),
    7,
    'getMinRequiredDepth'
  )

  const tangle = new MsgV3.Tangle(mootID)
  tangle.add(mootID, moot)
  await p(peer.db.add)(moot, mootID)

  const msg = MsgV3.create({
    keypair: aliceKeypair,
    domain: 'record_v1__profile',
    account: aliceID,
    accountTips: [aliceID],
    data: { update: { age: 2 }, supersedes: [] },
    tangles: {
      [mootID]: tangle,
    },
  })
  const rec = await p(peer.db.add)(msg, mootID)
  assert.equal(rec.id, 'XZWr3DZFG253awsWXgSkS2', 'msg ID')

  const fieldRoots7 = peer.record.getFieldRoots('profile')
  assert.deepEqual(
    fieldRoots7,
    {
      name: ['Y4JkpPCHN8Avtz4VALaAmK'],
      age: ['Y4JkpPCHN8Avtz4VALaAmK', rec.id],
    },
    'fieldRoots'
  )

  assert.equal(
    peer.record.getMinRequiredDepth(mootID),
    1,
    'getMinRequiredDepth'
  )

  assert.equal(
    peer.record._squeezePotential('profile'),
    6,
    'squeezePotential=6'
  )
})

test('teardown', async (t) => {
  await p(peer.close)(true)
})
