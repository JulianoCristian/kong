local Schema = require("kong.db.schema.init")
local DAO = require("kong.db.dao.init")
local errors = require("kong.db.errors")
local utils = require("kong.tools.utils")

local nullable_schema_definition = {
  name = "Foo",
  primary_key = { "a" },
  fields = {
    { a = { type = "number" }, },
    { b = { type = "string", default = "hello" }, },
    { u = { type = "string" }, },
    { r = { type = "record",
            fields = {
              { f1 = { type = "number" } },
              { f2 = { type = "string", default = "world" } },
            } } },
  }
}

local not_nullable_schema_definition = {
  name = "Foo",
  primary_key = { "a" },
  fields = {
    { a = { type = "number" }, },
    { b = { type = "string", default = "hello", nullable = false }, },
    { u = { type = "string" }, },
    { r = { type = "record",
            fields = {
              { f1 = { type = "number" } },
              { f2 = { type = "string", default = "world", nullable = false } },
            } } },
  }
}

local mock_db = {}

describe("DAO", function()

  describe("select", function()

    it("applies defaults if strategy returns column as nil and is nullable in schema", function()
      local schema = assert(Schema.new(nullable_schema_definition))

      -- mock strategy
      local strategy = {
        select = function()
          return { a = 42, b = nil, r = { f1 = 10 } }
        end,
      }

      local dao = DAO.new(mock_db, schema, strategy, errors)

      local row = dao:select({ a = 42 })
      assert.same(42, row.a)
      assert.same("hello", row.b)
      assert.same(10, row.r.f1)
      assert.same("world", row.r.f2)
    end)

    it("applies defaults if strategy returns column as nil and is not nullable in schema", function()
      local schema = assert(Schema.new(not_nullable_schema_definition))

      -- mock strategy
      local strategy = {
        select = function()
          return { a = 42, b = nil, r = { f1 = 10 } }
        end,
      }

      local dao = DAO.new(mock_db, schema, strategy, errors)

      local row = dao:select({ a = 42 })
      assert.same(42, row.a)
      assert.same("hello", row.b)
      assert.same(10, row.r.f1)
      assert.same("world", row.r.f2)
    end)

    it("applies defaults if strategy returns column as null and is not nullable in schema", function()
      local schema = assert(Schema.new(not_nullable_schema_definition))

      -- mock strategy
      local strategy = {
        select = function()
          return { a = 42, b = ngx.null, r = { f1 = 10, f2 = ngx.null } }
        end,
      }

      local dao = DAO.new(mock_db, schema, strategy, errors)

      local row = dao:select({ a = 42 })
      assert.same(42, row.a)
      assert.same("hello", row.b)
      assert.same(10, row.r.f1)
      assert.same("world", row.r.f2)
    end)

    it("preserves null if strategy returns column as null and is nullable in schema", function()
      local schema = assert(Schema.new(nullable_schema_definition))

      -- mock strategy
      local strategy = {
        select = function()
          return { a = 42, b = ngx.null, r = { f1 = 10, f2 = ngx.null } }
        end,
      }

      local dao = DAO.new(mock_db, schema, strategy, errors)

      local row = dao:select({ a = 42 }, { nulls = true })
      assert.same(42, row.a)
      assert.same(ngx.null, row.b)
      assert.same(10, row.r.f1)
      assert.same(ngx.null, row.r.f2)
    end)
  end)

  describe("update", function()

    it("does not pre-apply defaults on partial update if field is nullable in schema", function()
      local schema = assert(Schema.new(nullable_schema_definition))

      -- mock strategy
      local data
      local strategy = {
        update = function(_, _, value)
          -- no defaults pre-applied before partial update
          assert(value.b == nil)
          assert(value.r == nil or value.r.f2 == nil)
          data = utils.deep_merge(data, value)
          return data
        end,
      }

      local dao = DAO.new(mock_db, schema, strategy, errors)

      data = { a = 42, b = nil, u = nil, r = nil }
      local row, err = dao:update({ a = 42 }, { u = "foo" })
      assert.falsy(err)
      -- defaults are applied when returning the full updated entity
      assert.same({ a = 42, b = "hello", u = "foo", r = nil }, row)

      -- likewise for partial record update:

      data = { a = 42, b = nil, u = nil, r = nil }
      row, err = dao:update({ a = 43 }, { u = "foo", r = { f1 = 10 } })
      assert.falsy(err)
      assert.same({ a = 42, b = "hello", u = "foo", r = { f1 = 10, f2 = "world" } }, row)
    end)

    it("does not pre-apply defaults on partial update if field is not nullable in schema", function()
      local schema = assert(Schema.new(not_nullable_schema_definition))

      -- mock strategy
      local data
      local strategy = {
        update = function(_, _, value)
          -- no defaults pre-applied before partial update
          assert(value.b == nil)
          assert(value.r == nil or value.r.f2 == nil)
          data = utils.deep_merge(data, value)
          return data
        end,
      }

      local dao = DAO.new(mock_db, schema, strategy, errors)

      data = { a = 42, b = nil, u = nil, r = nil }
      local row, err = dao:update({ a = 42 }, { u = "foo" })
      assert.falsy(err)
      -- defaults are applied when returning the full updated entity
      assert.same({ a = 42, b = "hello", u = "foo", r = nil }, row)

      -- likewise for partial record update:

      data = { a = 42, b = nil, u = nil, r = nil }
      row, err = dao:update({ a = 43 }, { u = "foo", r = { f1 = 10 } })
      assert.falsy(err)
      assert.same({ a = 42, b = "hello", u = "foo", r = { f1 = 10, f2 = "world" } }, row)
    end)

    it("applies defaults if strategy returns column as null and is not nullable in schema", function()
      local schema = assert(Schema.new(not_nullable_schema_definition))

      -- mock strategy
      local strategy = {
        update = function()
          return { a = 42, b = ngx.null, r = { f1 = 10, f2 = ngx.null } }
        end,
      }

      local dao = DAO.new(mock_db, schema, strategy, errors)

      local row = dao:update({ a = 42 }, { u = "foo" })
      assert.same(42, row.a)
      assert.same("hello", row.b)
      assert.same(10, row.r.f1)
      assert.same("world", row.r.f2)
    end)

    it("preserves null if strategy returns column as null and is nullable in schema", function()
      local schema = assert(Schema.new(nullable_schema_definition))

      -- mock strategy
      local strategy = {
        update = function()
          return { a = 42, b = ngx.null, r = { f1 = 10, f2 = ngx.null } }
        end,
      }

      local dao = DAO.new(mock_db, schema, strategy, errors)

      local row = dao:update({ a = 42 }, { u = "foo" }, { nulls = true })
      assert.same(42, row.a)
      assert.same(ngx.null, row.b)
      assert.same(10, row.r.f1)
      assert.same(ngx.null, row.r.f2)
    end)
  end)

end)
