const test = require('node:test')
const assert = require('node:assert')
const path = require('path')
const os = require('os')
const rimraf = require('rimraf')
const MsgV4 = require('ppppp-db/msg-v4')
const Keypair = require('ppppp-keypair')
const p = require('util').promisify
const { createPeer } = require('./util')

const DIR = path.join(os.tmpdir(), 'ppppp-dict')
rimraf.sync(DIR)

const aliceKeypair = Keypair.generate('ed25519', 'alice')

function getMsgID(peer, index, domain) {
  let i = 0
  for (const rec of peer.db.records()) {
    if (rec.msg.metadata.domain === domain && !!rec.msg.data) {
      if (i === index) return rec.id
      i++
    }
  }
  throw new Error('msg not found')
}

let peer
let aliceID
test('setup', async (t) => {
  peer = createPeer({
    global: {
      keypair: aliceKeypair,
      path: DIR,
    },
    dict: { ghostSpan: 40 },
  })

  await peer.db.loaded()

  aliceID = await p(peer.db.account.create)({
    subdomain: 'account',
    _nonce: 'alice',
  })
  await p(peer.dict.load)(aliceID)

  peer.dict.setGhostSpan(4)
  assert.equal(peer.dict.getGhostSpan(), 4, 'getGhostSpan')
})

test('Dict update() and get()', async (t) => {
  assert(
    await p(peer.dict.update)('profile', { name: 'alice' }),
    'update .name'
  )
  const UPDATE0_ID = getMsgID(peer, 0, 'dict_v1__profile')
  assert.deepEqual(peer.dict.read(aliceID, 'profile'), { name: 'alice' }, 'get')

  const fieldRoots1 = peer.dict._getFieldRoots('profile')
  assert.deepEqual(fieldRoots1, { name: [UPDATE0_ID] }, 'fieldRoots')

  assert(await p(peer.dict.update)('profile', { age: 20 }), 'update .age')
  const UPDATE1_ID = getMsgID(peer, 1, 'dict_v1__profile')
  assert.deepEqual(
    peer.dict.read(aliceID, 'profile'),
    { name: 'alice', age: 20 },
    'get'
  )

  const fieldRoots2 = peer.dict._getFieldRoots('profile')
  assert.deepEqual(
    fieldRoots2,
    { name: [UPDATE0_ID], age: [UPDATE1_ID] },
    'fieldRoots'
  )

  assert.equal(
    await p(peer.dict.update)('profile', { name: 'alice' }),
    false,
    'redundant update .name'
  )
  assert.deepEqual(
    peer.dict.read(aliceID, 'profile'),
    { name: 'alice', age: 20 },
    'get'
  )

  assert.equal(
    await p(peer.dict.update)('profile', { name: 'Alice' }),
    true,
    'update .name'
  )
  const UPDATE2_ID = getMsgID(peer, 2, 'dict_v1__profile')
  assert.deepEqual(
    peer.dict.read(aliceID, 'profile'),
    { name: 'Alice', age: 20 },
    'get'
  )

  const fieldRoots3 = peer.dict._getFieldRoots('profile')
  assert.deepEqual(
    fieldRoots3,
    { age: [UPDATE1_ID], name: [UPDATE2_ID] },
    'fieldRoots'
  )
})

test('Dict squeeze', async (t) => {
  assert(await p(peer.dict.update)('profile', { age: 21 }), 'update .age')
  assert(await p(peer.dict.update)('profile', { age: 22 }), 'update .age')
  assert(await p(peer.dict.update)('profile', { age: 23 }), 'update .age')
  const UPDATE2_ID = getMsgID(peer, 2, 'dict_v1__profile')
  const UPDATE5_ID = getMsgID(peer, 5, 'dict_v1__profile')

  const fieldRoots4 = peer.dict._getFieldRoots('profile')
  assert.deepEqual(
    fieldRoots4,
    { name: [UPDATE2_ID], age: [UPDATE5_ID] },
    'fieldRoots'
  )

  assert.equal(peer.dict._squeezePotential('profile'), 3, 'squeezePotential=3')
  assert.equal(await p(peer.dict.squeeze)('profile'), true, 'squeezed')
  const UPDATE6_ID = getMsgID(peer, 6, 'dict_v1__profile')

  const fieldRoots5 = peer.dict._getFieldRoots('profile')
  assert.deepEqual(
    fieldRoots5,
    { name: [UPDATE6_ID], age: [UPDATE6_ID] },
    'fieldRoots'
  )

  assert.equal(peer.dict._squeezePotential('profile'), 0, 'squeezePotential=0')
  assert.equal(
    await p(peer.dict.squeeze)('profile'),
    false,
    'squeeze idempotent'
  )

  const fieldRoots6 = peer.dict._getFieldRoots('profile')
  assert.deepEqual(fieldRoots6, fieldRoots5, 'fieldRoots')
})

test('Dict isGhostable', (t) => {
  const moot = MsgV4.createMoot(aliceID, 'dict_v1__profile', aliceKeypair)
  const mootID = MsgV4.getMsgID(moot)

  assert.equal(mootID, peer.dict.getFeedID('profile'), 'getFeedID')

  const tangle = peer.db.getTangle(mootID)
  const msgIDs = tangle.topoSort()

  const fieldRoots = peer.dict._getFieldRoots('profile')
  assert.deepEqual(fieldRoots.age, [msgIDs[7]])

  // Remember from the setup, that ghostSpan=4
  assert.equal(msgIDs.length, 8)
  assert.equal(peer.dict.isGhostable(msgIDs[0], mootID), false) // moot
  assert.equal(peer.dict.isGhostable(msgIDs[1], mootID), false)
  assert.equal(peer.dict.isGhostable(msgIDs[2], mootID), false)
  assert.equal(peer.dict.isGhostable(msgIDs[3], mootID), true) // in ghostSpan
  assert.equal(peer.dict.isGhostable(msgIDs[4], mootID), true) // in ghostSpan
  assert.equal(peer.dict.isGhostable(msgIDs[5], mootID), true) // in ghostSpan
  assert.equal(peer.dict.isGhostable(msgIDs[6], mootID), true) // in ghostSpan
  assert.equal(peer.dict.isGhostable(msgIDs[7], mootID), false) // field root
})

test('Dict receives old branched update', async (t) => {
  const UPDATE6_ID = getMsgID(peer, 6, 'dict_v1__profile')

  const moot = MsgV4.createMoot(aliceID, 'dict_v1__profile', aliceKeypair)
  const mootID = MsgV4.getMsgID(moot)

  assert.equal(peer.dict.minRequiredDepth(mootID), 7, 'minRequiredDepth')

  const tangle = new MsgV4.Tangle(mootID)
  tangle.add(mootID, moot)
  await p(peer.db.add)(moot, mootID)

  const msg = MsgV4.create({
    keypair: aliceKeypair,
    domain: 'dict_v1__profile',
    account: aliceID,
    accountTips: [aliceID],
    data: { update: { age: 2 }, supersedes: [] },
    tangles: {
      [mootID]: tangle,
    },
  })
  const rec = await p(peer.db.add)(msg, mootID)

  const fieldRoots7 = peer.dict._getFieldRoots('profile')
  assert.deepEqual(
    fieldRoots7,
    {
      name: [UPDATE6_ID],
      age: [UPDATE6_ID, rec.id],
    },
    'fieldRoots'
  )

  assert.equal(peer.dict.minRequiredDepth(mootID), 1, 'minRequiredDepth')

  assert.equal(peer.dict._squeezePotential('profile'), 6, 'squeezePotential=6')
})

test('teardown', async (t) => {
  await p(peer.close)(true)
})
