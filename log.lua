local util = require("util")
local log = {}

function log.N(fmt, ...)
	printf("N: " .. fmt, ...)
end

function log.W(fmt, ...)
	printf("W: " .. fmt, ...)
end

function log.E(fmt, ...)
	printf("E [%s]: " .. fmt, util.caller(), ...)
end

function log.log(fmt, ...)
	printf(":: " .. fmt, ...)
end

setmetatable(log, { __call = log.log })
return log
