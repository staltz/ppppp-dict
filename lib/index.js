const MsgV3 = require('ppppp-db/msg-v3')

const PREFIX = 'record_v1__'

/**
 * @typedef {string} Subdomain
 * @typedef {string} MsgID
 * @typedef {`${Subdomain}.${string}`} SubdomainField
 */

/**
 * @param {string} domain
 * @returns {Subdomain}
 */
function toSubdomain(domain) {
  return domain.slice(PREFIX.length)
}

/**
 * @param {Subdomain} subdomain
 * @returns {string}
 */
function fromSubdomain(subdomain) {
  return PREFIX + subdomain
}

module.exports = {
  name: 'record',
  manifest: {},
  init(peer, config) {
    //#region state
    let accountID = /** @type {string | null} */ (null)
    let cancelListeningToRecordAdded = null
    let loadPromise = /** @type {Promise<void> | null} */ (null)
    const tangles = /** @type {Map<Subdomain, unknown>} */ (new Map())

    const fieldRoots = {
      /** @type {Map<SubdomainField, Set<MsgID>} */
      _map: new Map(),
      _getKey(subdomain, field) {
        return subdomain + '.' + field
      },
      get(subdomain, field = null) {
        if (field) {
          const key = this._getKey(subdomain, field)
          return this._map.get(key)
        } else {
          const out = {}
          for (const [key, value] of this._map.entries()) {
            if (key.startsWith(subdomain + '.')) {
              const field = key.slice(subdomain.length + 1)
              out[field] = [...value]
            }
          }
          return out
        }
      },
      add(subdomain, field, msgID) {
        const key = this._getKey(subdomain, field)
        const set = this._map.get(key) ?? new Set()
        set.add(msgID)
        return this._map.set(key, set)
      },
      del(subdomain, field, msgID) {
        const key = this._getKey(subdomain, field)
        const set = this._map.get(key)
        if (!set) return false
        set.delete(msgID)
        if (set.size === 0) this._map.delete(key)
        return true
      },
      toString() {
        return this._map
      },
    }
    //#endregion

    //#region active processes
    peer.close.hook(function (fn, args) {
      cancelListeningToRecordAdded()
      fn.apply(this, args)
    })
    //#endregion

    //#region internal methods
    function isValidRecordRootMsg(msg) {
      if (!msg) return false
      if (msg.metadata.account !== accountID) return false
      const domain = msg.metadata.domain
      if (!domain.startsWith(PREFIX)) return false
      return MsgV3.isMoot(msg, accountID, domain)
    }

    function isValidRecordMsg(msg) {
      if (!msg) return false
      if (!msg.data) return false
      if (msg.metadata.account !== accountID) return false
      if (!msg.metadata.domain.startsWith(PREFIX)) return false
      if (!msg.data.update) return false
      if (typeof msg.data.update !== 'object') return false
      if (Array.isArray(msg.data.update)) return false
      if (!Array.isArray(msg.data.supersedes)) return false
      return true
    }

    function learnRecordRoot(rootID, root) {
      const subdomain = toSubdomain(root.metadata.domain)
      const tangle = tangles.get(subdomain) ?? new MsgV3.Tangle(rootID)
      tangle.add(rootID, root)
      tangles.set(subdomain, tangle)
    }

    function learnRecordUpdate(msgID, msg) {
      const { account, domain } = msg.metadata
      const rootID = MsgV3.getMootID(account, domain)
      const subdomain = toSubdomain(domain)
      const tangle = tangles.get(subdomain) ?? new MsgV3.Tangle(rootID)
      tangle.add(msgID, msg)
      tangles.set(subdomain, tangle)

      for (const field in msg.data.update) {
        const existing = fieldRoots.get(subdomain, field)
        if (!existing) {
          fieldRoots.add(subdomain, field, msgID)
        } else {
          for (const existingID of existing) {
            if (tangle.precedes(existingID, msgID)) {
              fieldRoots.del(subdomain, field, existingID)
              fieldRoots.add(subdomain, field, msgID)
            } else {
              fieldRoots.add(subdomain, field, msgID)
            }
          }
        }
      }
    }

    function maybeLearnAboutRecord(msgID, msg) {
      if (msg.metadata.account !== accountID) return
      if (isValidRecordRootMsg(msg)) {
        learnRecordRoot(msgID, msg)
        return
      }
      if (isValidRecordMsg(msg)) {
        learnRecordUpdate(msgID, msg)
        return
      }
    }

    function loaded(cb) {
      if (cb === void 0) return loadPromise
      else loadPromise.then(() => cb(null), cb)
    }

    function _squeezePotential(subdomain) {
      // TODO: improve this so that the squeezePotential is the size of the
      // tangle suffix built as a slice from the fieldRoots
      const mootID = MsgV3.getMootID(accountID, fromSubdomain(subdomain))
      const tangle = peer.db.getTangle(mootID)
      const maxDepth = tangle.maxDepth
      const fieldRoots = getFieldRoots(accountID, subdomain)
      let minDepth = Infinity
      for (const field in fieldRoots) {
        for (const msgID of fieldRoots[field]) {
          const depth = tangle.getDepth(msgID)
          if (depth < minDepth) minDepth = depth
        }
      }
      return maxDepth - minDepth
    }

    function forceUpdate(subdomain, update, cb) {
      const domain = fromSubdomain(subdomain)

      // Populate supersedes
      const supersedes = []
      for (const field in update) {
        const existing = fieldRoots.get(subdomain, field)
        if (existing) supersedes.push(...existing)
      }

      peer.db.feed.publish(
        { account: accountID, domain, data: { update, supersedes } },
        (err, rec) => {
          // prettier-ignore
          if (err) return cb(new Error('Failed to create msg when force updating Record', { cause: err }))
          cb(null, true)
        }
      )
    }
    //#endregion

    //#region public methods

    /**
     * @param {string} id
     */
    function load(id, cb) {
      accountID = id
      loadPromise = new Promise((resolve, reject) => {
        for (const { id, msg } of peer.db.records()) {
          maybeLearnAboutRecord(id, msg)
        }
        cancelListeningToRecordAdded = peer.db.onRecordAdded(({ id, msg }) => {
          maybeLearnAboutRecord(id, msg)
        })
        resolve()
        cb()
      })
    }

    function getFieldRoots(id, subdomain) {
      // prettier-ignore
      if (id !== accountID) return cb(new Error(`Cannot getFieldRoots for another user's record. Given ID was "${id}"`))
      return fieldRoots.get(subdomain)
    }

    function get(id, subdomain) {
      const domain = fromSubdomain(subdomain)
      const mootID = MsgV3.getMootID(id, domain)
      const tangle = peer.db.getTangle(mootID)
      if (!tangle || tangle.size === 0) return {}
      const msgIDs = tangle.topoSort()
      const record = {}
      for (const msgID of msgIDs) {
        const msg = peer.db.get(msgID)
        if (isValidRecordMsg(msg)) {
          const { update } = msg.data
          Object.assign(record, update)
        }
      }
      return record
    }

    function update(id, subdomain, update, cb) {
      // prettier-ignore
      if (id !== accountID) return cb(new Error(`Cannot update another user's record. Given ID was "${id}"`))

      loaded(() => {
        const record = get(id, subdomain)

        let hasChanges = false
        for (const [field, value] of Object.entries(update)) {
          if (value !== record[field]) {
            hasChanges = true
            break
          }
        }
        if (!hasChanges) return cb(null, false)
        forceUpdate(subdomain, update, cb)
      })
    }

    function squeeze(id, subdomain, cb) {
      // prettier-ignore
      if (id !== accountID) return cb(new Error(`Cannot squeeze another user's record. Given ID was "${id}"`))
      const potential = _squeezePotential(subdomain)
      if (potential < 1) return cb(null, false)

      loaded(() => {
        const record = get(id, subdomain)
        forceUpdate(subdomain, record, (err) => {
          // prettier-ignore
          if (err) return cb(new Error('Failed to force update when squeezing Record', { cause: err }))
          cb(null, true)
        })
      })
    }
    //#endregion

    return {
      load,
      update,
      get,
      getFieldRoots,
      squeeze,

      _squeezePotential,
    }
  },
}
