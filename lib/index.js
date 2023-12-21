const MsgV3 = require('ppppp-db/msg-v3')

const PREFIX = 'dict_v1__'

/**
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
 * @typedef {string} Subdomain
 * @typedef {string} MsgID
 * @typedef {`${Subdomain}.${string}`} SubdomainField
 * @typedef {{
 *   update: {
 *     [field in string]: any
 *   },
 *   supersedes: Array<MsgID>,
 * }} DictMsgData
 * @typedef {{
 *   dict?: {
 *     ghostSpan?: number
 *   }
 * }} Config
 */

/**
 * @template [T = any]
 * @typedef {import('ppppp-db/msg-v3').Msg<T>} Msg<T>
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
function assertDBPlugin(peer) {
  if (!peer.db) throw new Error('dict plugin requires ppppp-db plugin')
}

/**
 * @param {{ db: PPPPPDB | null, close: ClosableHook }} peer
 * @param {Config} config
 */
function initDict(peer, config) {
  assertDBPlugin(peer)

  let ghostSpan = config.dict?.ghostSpan ?? 32
  if (ghostSpan < 1) throw new Error('config.dict.ghostSpan must be >= 0')

  //#region state
  let accountID = /** @type {string | null} */ (null)
  let loadPromise = /** @type {Promise<void> | null} */ (null)
  let cancelOnRecordAdded = /** @type {CallableFunction | null} */ (null)
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
     * @returns {{[field in string]: Array<MsgID>}}
     */
    getAll(subdomain) {
      const out = /** @type {{[field in string]: Array<MsgID>}} */ ({})
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
  function isValidDictMoot(msg) {
    if (!msg) return false
    if (msg.metadata.account !== accountID) return false
    const domain = msg.metadata.domain
    if (!domain.startsWith(PREFIX)) return false
    return MsgV3.isMoot(msg, accountID, domain)
  }

  /**
   * @private
   * @param {Msg | null | undefined} msg
   * @returns {msg is Msg<DictMsgData>}
   */
  function isValidDictMsg(msg) {
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
   * @param {string} mootID
   * @param {Msg} moot
   */
  function learnDictMoot(mootID, moot) {
    const subdomain = toSubdomain(moot.metadata.domain)
    const tangle = tangles.get(subdomain) ?? new MsgV3.Tangle(mootID)
    tangle.add(mootID, moot)
    tangles.set(subdomain, tangle)
  }

  /**
   * @private
   * @param {string} msgID
   * @param {Msg<DictMsgData>} msg
   */
  function learnDictUpdate(msgID, msg) {
    const { account, domain } = msg.metadata
    const mootID = MsgV3.getMootID(account, domain)
    const subdomain = toSubdomain(domain)
    const tangle = tangles.get(subdomain) ?? new MsgV3.Tangle(mootID)
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
  function maybeLearnAboutDict(msgID, msg) {
    if (msg.metadata.account !== accountID) return
    if (isValidDictMoot(msg)) {
      learnDictMoot(msgID, msg)
      return
    }
    if (isValidDictMsg(msg)) {
      learnDictUpdate(msgID, msg)
      return
    }
  }

  /**
   * @private
   * @param {CB<void>} cb
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
    assertDBPlugin(peer)
    if (!accountID) throw new Error('Cannot squeeze potential before loading')
    // TODO: improve this so that the squeezePotential is the size of the
    // tangle suffix built as a slice from the fieldRoots
    const mootID = MsgV3.getMootID(accountID, fromSubdomain(subdomain))
    const tangle = peer.db.getTangle(mootID)
    const maxDepth = tangle.maxDepth
    const fieldRoots = _getFieldRoots(subdomain)
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
   * @param {{[field in string]: any}} update
   * @param {CB<boolean>} cb
   */
  function forceUpdate(subdomain, update, cb) {
    assertDBPlugin(peer)
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
        if (err) return cb(new Error('Failed to create msg when force-updating Dict', { cause: err }))
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
    assertDBPlugin(peer)
    accountID = id
    loadPromise = new Promise((resolve, reject) => {
      for (const rec of peer.db.records()) {
        if (!rec.msg) continue
        maybeLearnAboutDict(rec.id, rec.msg)
      }
      cancelOnRecordAdded = peer.db.onRecordAdded(
        (/** @type {RecPresent} */ rec) => {
          try {
            maybeLearnAboutDict(rec.id, rec.msg)
          } catch (err) {
            console.error(err)
          }
        }
      )
      resolve()
      cb()
    })
  }

  /**
   * @param {string} subdomain
   */
  function _getFieldRoots(subdomain) {
    if (!accountID) throw new Error('Cannot getFieldRoots() before loading')
    return fieldRoots.getAll(subdomain)
  }

  /**
   * @public
   * @param {string} tangleID
   * @returns {number}
   */
  function minRequiredDepth(tangleID) {
    assertDBPlugin(peer)
    const tangle = peer.db.getTangle(tangleID)

    // prettier-ignore
    if (!tangle || tangle.size === 0) throw new Error(`Tangle "${tangleID}" is locally unknown`)
    // prettier-ignore
    if (!MsgV3.isMoot(tangle.root)) throw new Error(`Tangle "${tangleID}" is not a moot`)
    const domain = tangle.root.metadata.domain
    // prettier-ignore
    if (!domain.startsWith(PREFIX)) throw new Error(`Tangle "${tangleID}" is not a Dict moot`)

    // Discover field roots
    const fieldRoots = new Set()
    const msgIDs = tangle.topoSort()
    for (const msgID of msgIDs) {
      const msg = peer.db.get(msgID)
      if (!msg?.data) continue
      for (const supersededMsgID of msg.data.supersedes) {
        fieldRoots.delete(supersededMsgID)
      }
      fieldRoots.add(msgID)
    }

    // Get minimum depth of all field roots
    let minDepth = Infinity
    for (const msgID of fieldRoots) {
      const depth = tangle.getDepth(msgID)
      if (depth < minDepth) minDepth = depth
    }

    return minDepth
  }

  /**
   * @public
   * @param {string} tangleID
   * @returns {number}
   */
  function minGhostDepth(tangleID) {
    return Math.max(0, minRequiredDepth(tangleID) - ghostSpan)
  }

  /**
   * @public
   * @param {string} id
   * @param {string} subdomain
   * @returns {{[field in string]: any} | null}
   */
  function read(id, subdomain) {
    assertDBPlugin(peer)
    const domain = fromSubdomain(subdomain)
    const mootID = MsgV3.getMootID(id, domain)
    const tangle = peer.db.getTangle(mootID)
    if (!tangle || tangle.size === 0) {
      if (id === accountID) return {}
      else return null
    }
    const msgIDs = tangle.topoSort()
    const dict = /** @type {{[field in string]: any}} */ ({})
    for (const msgID of msgIDs) {
      const msg = peer.db.get(msgID)
      if (isValidDictMsg(msg)) {
        const { update } = msg.data
        Object.assign(dict, update)
      }
    }
    return dict
  }

  /**
   * @public
   * @param {string} subdomain
   * @returns {string}
   */
  function getFeedID(subdomain) {
    if (!accountID) throw new Error('Cannot getFeedID() before loading')
    assertDBPlugin(peer)
    const domain = fromSubdomain(subdomain)
    return MsgV3.getMootID(accountID, domain)
  }

  /**
   * @public
   * @param {MsgID} ghostableMsgID
   * @param {MsgID} tangleID
   */
  function isGhostable(ghostableMsgID, tangleID) {
    if (ghostableMsgID === tangleID) return false

    assertDBPlugin(peer)
    const msg = peer.db.get(ghostableMsgID)

    // prettier-ignore
    if (!msg) throw new Error(`isGhostable() msgID "${ghostableMsgID}" does not exist in the database`)

    const minFieldRootDepth = minRequiredDepth(tangleID)
    const minGhostDepth = minFieldRootDepth - ghostSpan
    const msgDepth = msg.metadata.tangles[tangleID].depth
    if (minGhostDepth <= msgDepth && msgDepth < minFieldRootDepth) return true
    return false
  }

  /**
   * @returns {number}
   */
  function getGhostSpan() {
    return ghostSpan
  }

  /**
   * @param {number} span
   * @returns {void}
   */
  function setGhostSpan(span) {
    if (span < 1) throw new Error('ghostSpan must be >= 0')
    ghostSpan = span
  }

  /**
   * @public
   * @param {string} subdomain
   * @param {{[field in string]: any}} update
   * @param {CB<boolean>} cb
   */
  function update(subdomain, update, cb) {
    if (!accountID) return cb(new Error('Cannot update before loading'))

    loaded(() => {
      if (!accountID) return cb(new Error('Expected account to be loaded'))
      const dict = read(accountID, subdomain)
      // prettier-ignore
      if (!dict) return cb(new Error(`Cannot update non-existent dict "${subdomain}`))

      let hasChanges = false
      for (const [field, value] of Object.entries(update)) {
        if (value !== dict[field]) {
          hasChanges = true
          break
        }
      }
      if (!hasChanges) return cb(null, false)
      forceUpdate(subdomain, update, cb)
    })
  }

  /**
   * @param {string} subdomain
   * @param {CB<boolean>} cb
   */
  function squeeze(subdomain, cb) {
    if (!accountID) return cb(new Error('Cannot squeeze before loading'))
    const potential = _squeezePotential(subdomain)
    if (potential < 1) return cb(null, false)

    loaded(() => {
      if (!accountID) return cb(new Error('Expected account to be loaded'))
      const dict = read(accountID, subdomain)
      // prettier-ignore
      if (!dict) return cb(new Error(`Cannot squeeze non-existent Dict "${subdomain}"`))
      forceUpdate(subdomain, dict, (err, _forceUpdated) => {
        // prettier-ignore
        if (err) return cb(new Error(`Failed to force update when squeezing Dict "${subdomain}"`, { cause: err }))
        // @ts-ignore
        cb(null, true)
      })
    })
  }
  //#endregion

  return {
    load,
    update,
    read,
    getFeedID,
    isGhostable,
    getGhostSpan,
    setGhostSpan,
    minGhostDepth,
    minRequiredDepth,
    squeeze,

    _getFieldRoots,
    _squeezePotential,
  }
}

exports.name = 'dict'
exports.init = initDict
