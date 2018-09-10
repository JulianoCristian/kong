local Schema = require("kong.db.schema.init")
local DAO = require("kong.db.dao.init")
local errors = require("kong.db.errors")

describe("DAO", function()

  it("applies defaults if strategy returns column as nil and is nullable in schema", function()
    local schema = assert(Schema.new({
      name = "Foo",
      primary_key = { "a" },
      fields = {
        { a = { type = "number" }, },
        { b = { type = "string", default = "hello" }, },
        { r = { type = "record",
                fields = {
                  { f1 = { type = "number" } },
                  { f2 = { type = "string", default = "world" } },
                } } },
      }
    }))

    -- mock db
    local db = {}

    -- mock strategy
    local strategy = {
      select = function()
        return { a = 42, b = nil, r = { f1 = 10 } }
      end,
    }

    local dao = DAO.new(db, schema, strategy, errors)

    local row = dao:select({ a = 42 })
    assert.same(42, row.a)
    assert.same("hello", row.b)
    assert.same(10, row.r.f1)
    assert.same("world", row.r.f2)
  end)

  it("applies defaults if strategy returns column as nil and is not nullable in schema", function()
    local schema = assert(Schema.new({
      name = "Foo",
      primary_key = { "a" },
      fields = {
        { a = { type = "number" }, },
        { b = { type = "string", default = "hello", nullable = false }, },
        { r = { type = "record",
                fields = {
                  { f1 = { type = "number" } },
                  { f2 = { type = "string", default = "world", nullable = false } },
                } } },
      }
    }))

    -- mock db
    local db = {}

    -- mock strategy
    local strategy = {
      select = function()
        return { a = 42, b = nil, r = { f1 = 10 } }
      end,
    }

    local dao = DAO.new(db, schema, strategy, errors)

    local row = dao:select({ a = 42 })
    assert.same(42, row.a)
    assert.same("hello", row.b)
    assert.same(10, row.r.f1)
    assert.same("world", row.r.f2)
  end)

  it("applies defaults if strategy returns column as null and is not nullable in schema", function()
    local schema = assert(Schema.new({
      name = "Foo",
      primary_key = { "a" },
      fields = {
        { a = { type = "number" }, },
        { b = { type = "string", default = "hello", nullable = false }, },
        { r = { type = "record",
                fields = {
                  { f1 = { type = "number" } },
                  { f2 = { type = "string", default = "world", nullable = false } },
                } } },
      }
    }))

    -- mock db
    local db = {}

    -- mock strategy
    local strategy = {
      select = function()
        return { a = 42, b = ngx.null, r = { f1 = 10, f2 = ngx.null } }
      end,
    }

    local dao = DAO.new(db, schema, strategy, errors)

    local row = dao:select({ a = 42 })
    assert.same(42, row.a)
    assert.same("hello", row.b)
    assert.same(10, row.r.f1)
    assert.same("world", row.r.f2)
  end)

  it("preserves null if strategy returns column as null and is nullable in schema", function()
    local schema = assert(Schema.new({
      name = "Foo",
      primary_key = { "a" },
      fields = {
        { a = { type = "number" }, },
        { b = { type = "string", default = "hello", nullable = true }, },
        { r = { type = "record",
                fields = {
                  { f1 = { type = "number" } },
                  { f2 = { type = "string", default = "world", nullable = true } },
                } } },
      }
    }))

    -- mock db
    local db = {}

    -- mock strategy
    local strategy = {
      select = function()
        return { a = 42, b = ngx.null, r = { f1 = 10, f2 = ngx.null } }
      end,
    }

    local dao = DAO.new(db, schema, strategy, errors)

    local row = dao:select({ a = 42 }, { nulls = true })
    assert.same(42, row.a)
    assert.same(ngx.null, row.b)
    assert.same(10, row.r.f1)
    assert.same(ngx.null, row.r.f2)
  end)

end)
