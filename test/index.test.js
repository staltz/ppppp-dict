const test = require('node:test')
const assert = require('node:assert')
const path = require('path')
const os = require('os')
const rimraf = require('rimraf')
const MsgV3 = require('ppppp-db/msg-v3')
const Keypair = require('ppppp-keypair')
const p = require('util').promisify
const { createPeer } = require('./util')

const DIR = path.join(os.tmpdir(), 'ppppp-dict')
rimraf.sync(DIR)

const aliceKeypair = Keypair.generate('ed25519', 'alice')

let peer
let aliceID
test('setup', async (t) => {
  peer = createPeer({
    keypair: aliceKeypair,
    path: DIR,
    dict: { ghostSpan: 4 },
  })

  await peer.db.loaded()

  aliceID = await p(peer.db.account.create)({
    domain: 'account',
    _nonce: 'alice',
  })
  await p(peer.dict.load)(aliceID)

  assert.equal(peer.dict.getGhostSpan(), 4, 'getGhostSpan')
})

test('Dict update() and get()', async (t) => {
  assert(
    await p(peer.dict.update)('profile', { name: 'alice' }),
    'update .name'
  )
  assert.deepEqual(
    peer.dict.read(aliceID, 'profile'),
    { name: 'alice' },
    'get'
  )

  const fieldRoots1 = peer.dict._getFieldRoots('profile')
  assert.deepEqual(
    fieldRoots1,
    { name: ['QZSb3GMTRWWUUVLtueNB7Q'] },
    'fieldRoots'
  )

  assert(await p(peer.dict.update)('profile', { age: 20 }), 'update .age')
  assert.deepEqual(
    peer.dict.read(aliceID, 'profile'),
    { name: 'alice', age: 20 },
    'get'
  )

  const fieldRoots2 = peer.dict._getFieldRoots('profile')
  assert.deepEqual(
    fieldRoots2,
    { name: ['QZSb3GMTRWWUUVLtueNB7Q'], age: ['98QTF8Zip6NYJgmcf96L2K'] },
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
  assert.deepEqual(
    peer.dict.read(aliceID, 'profile'),
    { name: 'Alice', age: 20 },
    'get'
  )

  const fieldRoots3 = peer.dict._getFieldRoots('profile')
  assert.deepEqual(
    fieldRoots3,
    { age: ['98QTF8Zip6NYJgmcf96L2K'], name: ['49rg6mJFDgdq6kZTE8uedr'] },
    'fieldRoots'
  )
})

test('Dict squeeze', async (t) => {
  assert(await p(peer.dict.update)('profile', { age: 21 }), 'update .age')
  assert(await p(peer.dict.update)('profile', { age: 22 }), 'update .age')
  assert(await p(peer.dict.update)('profile', { age: 23 }), 'update .age')

  const fieldRoots4 = peer.dict._getFieldRoots('profile')
  assert.deepEqual(
    fieldRoots4,
    { name: ['49rg6mJFDgdq6kZTE8uedr'], age: ['GE9KcJc5efunBhSTDjy6zX'] },
    'fieldRoots'
  )

  assert.equal(
    peer.dict._squeezePotential('profile'),
    3,
    'squeezePotential=3'
  )
  assert.equal(await p(peer.dict.squeeze)('profile'), true, 'squeezed')

  const fieldRoots5 = peer.dict._getFieldRoots('profile')
  assert.deepEqual(
    fieldRoots5,
    { name: ['Xr7DZdwaANzPByUdRYGb2E'], age: ['Xr7DZdwaANzPByUdRYGb2E'] },
    'fieldRoots'
  )

  assert.equal(
    peer.dict._squeezePotential('profile'),
    0,
    'squeezePotential=0'
  )
  assert.equal(
    await p(peer.dict.squeeze)('profile'),
    false,
    'squeeze idempotent'
  )

  const fieldRoots6 = peer.dict._getFieldRoots('profile')
  assert.deepEqual(fieldRoots6, fieldRoots5, 'fieldRoots')
})

test('Dict isGhostable', (t) => {
  const moot = MsgV3.createMoot(aliceID, 'dict_v1__profile', aliceKeypair)
  const mootID = MsgV3.getMsgID(moot)

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
  const moot = MsgV3.createMoot(aliceID, 'dict_v1__profile', aliceKeypair)
  const mootID = MsgV3.getMsgID(moot)

  assert.equal(peer.dict.minRequiredDepth(mootID), 7, 'minRequiredDepth')

  const tangle = new MsgV3.Tangle(mootID)
  tangle.add(mootID, moot)
  await p(peer.db.add)(moot, mootID)

  const msg = MsgV3.create({
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
  assert.equal(rec.id, 'PBq5dgfK9icRVx7SLhyaC5', 'msg ID')

  const fieldRoots7 = peer.dict._getFieldRoots('profile')
  assert.deepEqual(
    fieldRoots7,
    {
      name: ['Xr7DZdwaANzPByUdRYGb2E'],
      age: ['Xr7DZdwaANzPByUdRYGb2E', rec.id],
    },
    'fieldRoots'
  )

  assert.equal(peer.dict.minRequiredDepth(mootID), 1, 'minRequiredDepth')

  assert.equal(
    peer.dict._squeezePotential('profile'),
    6,
    'squeezePotential=6'
  )
})

test('teardown', async (t) => {
  await p(peer.close)(true)
})
