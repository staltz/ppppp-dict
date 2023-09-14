const MsgV3 = require('ppppp-db/msg-v3')

const PREFIX = 'record_v1__'

/**
 * @typedef {import('ppppp-db').Msg} Msg
 * @typedef {ReturnType<import('ppppp-db').init>} PPPPPDB
 * @typedef {import('ppppp-db').RecPresent} RecPresent
 * @typedef {{
 *   hook: (
 *     cb: (
 *       this: any,
 *       fn: (this: any, ...a: Array<any>) => any,
 *       args: Array<any>
 *     ) => void
 *   ) => void
 * }} ClosableHook
 */

/**
 * @typedef {string} Subdomain
 * @typedef {string} MsgID
 * @typedef {`${Subdomain}.${string}`} SubdomainField
 */

/**
 * @template T
 * @typedef {T extends void ?
 *   (...args: [Error] | []) => void :
 *   (...args: [Error] | [null, T]) => void
 * } CB
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

/**
 * @param {{
 *   db: PPPPPDB | null,
 *   close: ClosableHook,
 * }} peer
 * @returns {asserts peer is { db: PPPPPDB, close: ClosableHook }}
 */
function assertDBExists(peer) {
  if (!peer.db) throw new Error('record plugin requires ppppp-db plugin')
}

module.exports = {
  name: 'record',
  manifest: {},

  /**
   * @param {{
   *   db: PPPPPDB | null,
   *   close: ClosableHook,
   * }} peer
   * @param {any} config
   */
  init(peer, config) {
    assertDBExists(peer)

    //#region state
    let accountID = /** @type {string | null} */ (null)
    let cancelOnRecordAdded = /** @type {CallableFunction | null} */ (null)
    let loadPromise = /** @type {Promise<void> | null} */ (null)
    const tangles = /** @type {Map<Subdomain, MsgV3.Tangle>} */ (new Map())

    const fieldRoots = {
      _map: /** @type {Map<SubdomainField, Set<MsgID>>} */ (new Map()),
      /**
       * @param {string} subdomain
       * @param {string} field
       * @returns {SubdomainField}
       */
      _getKey(subdomain, field) {
        return `${subdomain}.${field}`
      },
      /**
       * @param {string} subdomain
       * @returns {Record<string, Array<MsgID>>}
       */
      getAll(subdomain) {
        const out = /** @type {Record<string, Array<MsgID>>} */ ({})
        for (const [key, value] of this._map.entries()) {
          if (key.startsWith(subdomain + '.')) {
            const field = key.slice(subdomain.length + 1)
            out[field] = [...value]
          }
        }
        return out
      },
      /**
       * @param {string} subdomain
       * @param {string} field
       * @returns {Set<MsgID> | undefined}
       */
      get(subdomain, field) {
        const key = this._getKey(subdomain, field)
        return this._map.get(key)
      },
      /**
       * @param {string} subdomain
       * @param {string} field
       * @param {MsgID} msgID
       */
      add(subdomain, field, msgID) {
        const key = this._getKey(subdomain, field)
        const set = this._map.get(key) ?? new Set()
        set.add(msgID)
        return this._map.set(key, set)
      },
      /**
       * @param {string} subdomain
       * @param {string} field
       * @param {MsgID} msgID
       */
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
      cancelOnRecordAdded?.()
      fn.apply(this, args)
    })
    //#endregion

    //#region internal methods
    /**
     * @private
     * @param {Msg | null | undefined} msg
     * @returns {msg is Msg}
     */
    function isValidRecordMoot(msg) {
      if (!msg) return false
      if (msg.metadata.account !== accountID) return false
      const domain = msg.metadata.domain
      if (!domain.startsWith(PREFIX)) return false
      return MsgV3.isMoot(msg, accountID, domain)
    }

    /**
     * @private
     * @param {Msg | null | undefined} msg
     * @returns {msg is Msg}
     */
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

    /**
     * @private
     * @param {string} rootID
     * @param {Msg} root
     */
    function learnRecordRoot(rootID, root) {
      const subdomain = toSubdomain(root.metadata.domain)
      const tangle = tangles.get(subdomain) ?? new MsgV3.Tangle(rootID)
      tangle.add(rootID, root)
      tangles.set(subdomain, tangle)
    }

    /**
     * @private
     * @param {string} msgID
     * @param {Msg} msg
     */
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

    /**
     * @private
     * @param {string} msgID
     * @param {Msg} msg
     */
    function maybeLearnAboutRecord(msgID, msg) {
      if (msg.metadata.account !== accountID) return
      if (isValidRecordMoot(msg)) {
        learnRecordRoot(msgID, msg)
        return
      }
      if (isValidRecordMsg(msg)) {
        learnRecordUpdate(msgID, msg)
        return
      }
    }

    /**
     * @private
     * @param {CB<void>} cb
     * @returns
     */
    function loaded(cb) {
      if (cb === void 0) return loadPromise
      else loadPromise?.then(() => cb(), cb)
    }

    /**
     * @private
     * @param {string} subdomain
     * @returns {number}
     */
    function _squeezePotential(subdomain) {
      assertDBExists(peer)
      if (!accountID) throw new Error('Cannot squeeze potential before loading')
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

    /**
     * @param {string} subdomain
     * @param {Record<string, any>} update
     * @param {CB<boolean>} cb
     */
    function forceUpdate(subdomain, update, cb) {
      assertDBExists(peer)
      if (!accountID) throw new Error('Cannot force update before loading')
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
          // @ts-ignore
          cb(null, true)
        }
      )
    }
    //#endregion

    //#region public methods

    /**
     * @param {string} id
     * @param {CB<void>} cb
     */
    function load(id, cb) {
      assertDBExists(peer)
      accountID = id
      loadPromise = new Promise((resolve, reject) => {
        for (const rec of peer.db.records()) {
          if (!rec.msg) continue
          maybeLearnAboutRecord(rec.id, rec.msg)
        }
        cancelOnRecordAdded = peer.db.onRecordAdded(
          (/** @type {RecPresent} */ rec) => {
          if (!rec.msg) return
          maybeLearnAboutRecord(rec.id, rec.msg)
        })
        resolve()
        cb()
      })
    }

    /**
     * @param {string} id
     * @param {string} subdomain
     */
    function getFieldRoots(id, subdomain) {
      // prettier-ignore
      if (id !== accountID) throw new Error(`Cannot getFieldRoots for another user's record. Given ID was "${id}"`)
      return fieldRoots.getAll(subdomain)
    }

    /**
     * @public
     * @param {string} id
     * @param {string} subdomain
     */
    function get(id, subdomain) {
      assertDBExists(peer)
      const domain = fromSubdomain(subdomain)
      const mootID = MsgV3.getMootID(id, domain)
      const tangle = peer.db.getTangle(mootID)
      if (!tangle || tangle.size === 0) return {}
      const msgIDs = tangle.topoSort()
      const record = /** @type {Record<string, any>}*/ ({})
      for (const msgID of msgIDs) {
        const msg = peer.db.get(msgID)
        if (isValidRecordMsg(msg)) {
          const { update } = msg.data
          Object.assign(record, update)
        }
      }
      return record
    }

    /**
     * @public
     * @param {string} id
     * @param {string} subdomain
     * @param {Record<string, any>} update
     * @param {CB<boolean>} cb
     */
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

    /**
     * @param {string} id
     * @param {string} subdomain
     * @param {CB<boolean>} cb
     */
    function squeeze(id, subdomain, cb) {
      // prettier-ignore
      if (id !== accountID) return cb(new Error(`Cannot squeeze another user's record. Given ID was "${id}"`))
      const potential = _squeezePotential(subdomain)
      if (potential < 1) return cb(null, false)

      loaded(() => {
        const record = get(id, subdomain)
        forceUpdate(subdomain, record, (err, _forceUpdated) => {
          // prettier-ignore
          if (err) return cb(new Error('Failed to force update when squeezing Record', { cause: err }))
          // @ts-ignore
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
