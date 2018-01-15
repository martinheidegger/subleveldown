var util = require('util')
var encoding = require('encoding-down')
var wrap = require('level-option-wrap')

var END = Buffer.from([0xff])

var concat = function (prefix, key, force) {
  if (typeof key === 'string' && (force || key.length)) return prefix + key
  if (Buffer.isBuffer(key) && (force || key.length)) return Buffer.concat([Buffer.from(prefix), key])
  return key
}

var SubIterator = function (ite, prefix) {
  this.iterator = ite
  this.prefix = prefix
}

SubIterator.prototype.next = function (cb) {
  var self = this
  this.iterator.next(cb && function (err, key, value) {
    if (err) return cb(err)
    if (key) key = key.slice(self.prefix.length)
    cb.apply(null, arguments)
  })
}

SubIterator.prototype.end = function (cb) {
  this.iterator.end(cb)
}

var SubDown = function (db, prefix, opts) {
  if (!(this instanceof SubDown)) return new SubDown(db, prefix, opts)
  opts = opts || {}
  if (typeof opts === 'string') opts = {separator: opts}
  encoding.EncodingDOWN.call(this, [db, opts])

  var separator = opts.separator

  if (!prefix) prefix = ''
  if (!separator) separator = '!'
  if (prefix[0] === separator) prefix = prefix.slice(1)
  if (prefix[prefix.length - 1] === separator) prefix = prefix.slice(0, -1)

  this.db = db
  var rawPrefix = separator + prefix + separator
  this._beforeOpen = opts.open

  var self = this

  this._wrap = {
    gt: function (x) {
      return concat(self.prefix, x || '', true)
    },
    lt: function (x) {
      if (Buffer.isBuffer(x) && !x.length) x = END
      return concat(self.prefix, x || '\xff')
    }
  }

  var onOpen = function () {
    if (self.db.db.type === 'subdown' && self.db.db.prefix) {
      self.prefix = self.db.db.prefix + rawPrefix
      self.leveldown = self.db.db.leveldown
    } else {
      onClose()
    }
  }
  var onClose = function () {
    self.prefix = rawPrefix
    self.leveldown = self.db.db
  }

  this.db.on('open', onOpen)
  this.db.on('close', onClose)

  if (this.db.isOpen()) {
    onOpen()
  } else {
    onClose()
  }

  this.destroy = function () {
    self.db.off('open', onOpen)
    self.db.off('close', onClose)
    self.db.destroy()
  }
}

util.inherits(SubDown, encoding.EncodingDOWN)

SubDown.prototype.type = 'subdown'

SubDown.prototype.isOpen = function () {
  return this.db.isOpen()
}

SubDown.prototype._open = function (opts, cb) {
  this.db.open(cb)
}

SubDown.prototype._close = function () {
  this.db.close.apply(this.db, arguments)
}

SubDown.prototype._put = function (key, value, opts, cb) {
  this.leveldown.put(concat(this.prefix, key), value, opts, cb)
}

SubDown.prototype._get = function (key, opts, cb) {
  this.leveldown.get(concat(this.prefix, key), opts, cb)
}

SubDown.prototype._del = function (key, opts, cb) {
  this.leveldown.del(concat(this.prefix, key), opts, cb)
}

SubDown.prototype.batch =
SubDown.prototype._batch = function (operations, opts, cb) {
  if (arguments.length === 0) return new encoding.EncodingBatch(this)
  if (!Array.isArray(operations)) return this.leveldown.batch.apply(null, arguments)

  var subops = new Array(operations.length)
  for (var i = 0; i < operations.length; i++) {
    var o = operations[i]
    subops[i] = {type: o.type, key: concat(this.prefix, o.key), value: o.value}
  }

  return this.leveldown.batch(subops, opts, cb)
}

SubDown.prototype._approximateSize = function (start, end, cb) {
  return this.leveldown.approximateSize.apply(this.leveldown, arguments)
}

SubDown.prototype.getProperty = function () {
  return this.leveldown.getProperty.apply(this.leveldown, arguments)
}

SubDown.prototype.repair = function () {
  return this.leveldown.repair.apply(this.leveldown, arguments)
}

var extend = function (xopts, opts) {
  xopts.keys = opts.keys
  xopts.values = opts.values
  xopts.createIfMissing = opts.createIfMissing
  xopts.errorIfExists = opts.errorIfExists
  xopts.keyEncoding = opts.keyEncoding
  xopts.valueEncoding = opts.valueEncoding
  xopts.compression = opts.compression
  xopts.db = opts.db
  xopts.limit = opts.limit
  xopts.keyAsBuffer = opts.keyAsBuffer
  xopts.valueAsBuffer = opts.valueAsBuffer
  xopts.reverse = opts.reverse
  return xopts
}

var fixRange = function (opts) {
  return (!opts.reverse || (!opts.end && !opts.start)) ? opts : {start: opts.end, end: opts.start}
}

SubDown.prototype.iterator = function (opts) {
  if (!opts) opts = {}
  var xopts = extend(wrap(fixRange(opts), this._wrap), opts)
  return new SubIterator(this.leveldown.iterator(xopts), this.prefix)
}

module.exports = SubDown
