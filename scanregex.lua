--[[
Use SCAN to search the entire keyspace and filter keys
with Lua patterns which are not POSIX regex, but ATM
(v3) the best thing available in Redis.

KEYS: none
ARGV: 1: Lua pattern (defaults to .* if unprovided)
      2: Complement switch (i.e. not)
]]--

local re = ARGV[1]
local nt = ARGV[2]

local cur = 0
local rep = {}
local tmp

if not re then
  re = ".*"
end

repeat
  tmp = redis.call("SCAN", cur, "MATCH", "*")
  cur = tonumber(tmp[1])
  if tmp[2] then
    for k, v in pairs(tmp[2]) do
      local fi = v:find(re) 
      if (fi and not nt) or (not fi and nt) then
        rep[#rep+1] = v
      end
    end
  end
until cur == 0
return rep