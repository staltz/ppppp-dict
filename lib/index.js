const FeedV1 = require('ppppp-db/feed-v1')

const PREFIX = 'record_v1__'

/** @typedef {string} Subtype */

/** @typedef {string} MsgHash */

/** @typedef {`${Subtype}.${string}`} SubtypeField */

/**
 * @param {string} type
 * @returns {Subtype}
 */
function toSubtype(type) {
  return type.slice(PREFIX.length)
}

/**
 * @param {Subtype} subtype
 * @returns {string}
 */
function fromSubtype(subtype) {
  return PREFIX + subtype
}

module.exports = {
  name: 'record',
  manifest: {
    update: 'async',
    get: 'sync',
    squeeze: 'async',
  },
  init(peer, config) {
    //#region state
    const myWho = FeedV1.stripAuthor(config.keys.id)
    let cancelListeningToRecordAdded = null

    /** @type {Map<Subtype, unknown>} */
    const tangles = new Map()

    const fieldRoots = {
      /** @type {Map<SubtypeField, Set<MsgHash>} */
      _map: new Map(),
      _getKey(subtype, field) {
        return subtype + '.' + field
      },
      get(subtype, field = null) {
        if (field) {
          const key = this._getKey(subtype, field)
          return this._map.get(key)
        } else {
          const out = {}
          for (const [key, value] of this._map.entries()) {
            if (key.startsWith(subtype + '.')) {
              const field = key.slice(subtype.length + 1)
              out[field] = [...value]
            }
          }
          return out
        }
      },
      add(subtype, field, msgHash) {
        const key = this._getKey(subtype, field)
        const set = this._map.get(key) ?? new Set()
        set.add(msgHash)
        return this._map.set(key, set)
      },
      del(subtype, field, msgHash) {
        const key = this._getKey(subtype, field)
        const set = this._map.get(key)
        if (!set) return false
        set.delete(msgHash)
        if (set.size === 0) this._map.delete(key)
        return true
      },
      toString() {
        return this._map
      },
    }
    //#endregion

    //#region active processes
    const loadPromise = new Promise((resolve, reject) => {
      for (const { hash, msg } of peer.db.records()) {
        maybeLearnAboutRecord(hash, msg)
      }
      cancelListeningToRecordAdded = peer.db.onRecordAdded(({ hash, msg }) => {
        maybeLearnAboutRecord(hash, msg)
      })
      resolve()
    })

    peer.close.hook(function (fn, args) {
      cancelListeningToRecordAdded()
      fn.apply(this, args)
    })
    //#endregion

    //#region internal methods
    function isValidRecordRootMsg(msg) {
      if (!msg) return false
      if (msg.metadata.who !== myWho) return false
      const type = msg.metadata.type
      if (!type.startsWith(PREFIX)) return false
      return FeedV1.isFeedRoot(msg, config.keys.id, type)
    }

    function isValidRecordMsg(msg) {
      if (!msg) return false
      if (!msg.content) return false
      if (msg.metadata.who !== myWho) return false
      if (!msg.metadata.type.startsWith(PREFIX)) return false
      if (!msg.content.update) return false
      if (typeof msg.content.update !== 'object') return false
      if (Array.isArray(msg.content.update)) return false
      if (!Array.isArray(msg.content.supersedes)) return false
      return true
    }

    function learnRecordRoot(hash, msg) {
      const { type } = msg.metadata
      const subtype = toSubtype(type)
      const tangle = tangles.get(subtype) ?? new FeedV1.Tangle(hash)
      tangle.add(hash, msg)
      tangles.set(subtype, tangle)
    }

    function learnRecordUpdate(hash, msg) {
      const { who, type } = msg.metadata
      const rootHash = FeedV1.getFeedRootHash(who, type)
      const subtype = toSubtype(type)
      const tangle = tangles.get(subtype) ?? new FeedV1.Tangle(rootHash)
      tangle.add(hash, msg)
      tangles.set(subtype, tangle)

      for (const field in msg.content.update) {
        const existing = fieldRoots.get(subtype, field)
        if (!existing) {
          fieldRoots.add(subtype, field, hash)
        } else {
          for (const existingHash of existing) {
            if (tangle.precedes(existingHash, hash)) {
              fieldRoots.del(subtype, field, existingHash)
              fieldRoots.add(subtype, field, hash)
            } else {
              fieldRoots.add(subtype, field, hash)
            }
          }
        }
      }
    }

    function maybeLearnAboutRecord(hash, msg) {
      if (msg.metadata.who !== myWho) return
      if (isValidRecordRootMsg(msg)) {
        learnRecordRoot(hash, msg)
        return
      }
      if (isValidRecordMsg(msg)) {
        learnRecordUpdate(hash, msg)
        return
      }
    }

    function loaded(cb) {
      if (cb === void 0) return loadPromise
      else loadPromise.then(() => cb(null), cb)
    }

    function _getFieldRoots(subtype) {
      return fieldRoots.get(subtype)
    }

    function _squeezePotential(subtype) {
      const rootHash = FeedV1.getFeedRootHash(myWho, fromSubtype(subtype))
      const tangle = peer.db.getTangle(rootHash)
      const maxDepth = tangle.getMaxDepth()
      const fieldRoots = _getFieldRoots(subtype)
      let minDepth = Infinity
      for (const field in fieldRoots) {
        for (const msgHash of fieldRoots[field]) {
          const depth = tangle.getDepth(msgHash)
          if (depth < minDepth) minDepth = depth
        }
      }
      return maxDepth - minDepth
    }

    function forceUpdate(subtype, update, cb) {
      const type = fromSubtype(subtype)

      // Populate supersedes
      const supersedes = []
      for (const field in update) {
        const existing = fieldRoots.get(subtype, field)
        if (existing) supersedes.push(...existing)
      }

      peer.db.create({ type, content: { update, supersedes } }, (err, rec) => {
        // prettier-ignore
        if (err) return cb(new Error('Failed to create msg when force updating Record', { cause: err }))
        cb(null, true)
      })
    }
    //#endregion

    //#region public methods
    function get(authorId, subtype) {
      const type = fromSubtype(subtype)
      const rootHash = FeedV1.getFeedRootHash(authorId, type)
      const tangle = peer.db.getTangle(rootHash)
      if (!tangle || tangle.size() === 0) return {}
      const msgHashes = tangle.topoSort()
      const record = {}
      for (const msgHash of msgHashes) {
        const msg = peer.db.get(msgHash)
        if (isValidRecordMsg(msg)) {
          const { update } = msg.content
          Object.assign(record, update)
        }
      }
      return record
    }

    function update(authorId, subtype, update, cb) {
      const who = FeedV1.stripAuthor(authorId)
      // prettier-ignore
      if (who !== myWho) return cb(new Error('Cannot update another user\'s record. Given "authorId" was ' + authorId))

      loaded(() => {
        const record = get(authorId, subtype)

        let hasChanges = false
        for (const [field, value] of Object.entries(update)) {
          if (value !== record[field]) {
            hasChanges = true
            break
          }
        }
        if (!hasChanges) return cb(null, false)
        forceUpdate(subtype, update, cb)
      })
    }

    function squeeze(authorId, subtype, cb) {
      const who = FeedV1.stripAuthor(authorId)
      // prettier-ignore
      if (who !== myWho) return cb(new Error('Cannot squeeze another user\'s record. Given "authorId" was ' + authorId))
      const potential = _squeezePotential(subtype)
      if (potential < 1) return cb(null, false)

      loaded(() => {
        const record = get(authorId, subtype)
        forceUpdate(subtype, record, (err) => {
          // prettier-ignore
          if (err) return cb(new Error('Failed to force update when squeezing Record', { cause: err }))
          cb(null, true)
        })
      })
    }
    //#endregion

    return {
      update,
      get,
      squeeze,

      _getFieldRoots,
      _squeezePotential,
    }
  },
}
