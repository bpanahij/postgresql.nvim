local M = {}
local api = vim.api
local fn = vim.fn

-- At the top of your file, add:
local json = require("plenary.json")
local job = require("plenary.job")

-- The rest of your code remains the same...

-- Telescope requirements
local pickers = require("telescope.pickers")
local finders = require("telescope.finders")
local conf = require("telescope.config").values
local actions = require("telescope.actions")
local action_state = require("telescope.actions.state")

function M.setup(opts)
	opts = opts or {}
end

local function parse_connection_string(conn_string)
	local env = {}
	for key, value in conn_string:gmatch("(%w+)=([^;]+)") do
		env[key:lower()] = value
	end
	return env
end

local function splitCommandIntoTable(command)
	local cmd = {}
	for word in command:gmatch("%S+") do
		table.insert(cmd, word)
	end
	return cmd
end

local function loadConfigFromCommand(command, optionName, callback, defaultValue)
	local cmd = splitCommandIntoTable(command)
	job:new({
		command = cmd[1],
		args = vim.list_slice(cmd, 2, #cmd),
		on_exit = function(j, exit_code)
			if exit_code ~= 0 then
				return
			end
			local value = j:result()[1]:gsub("%s+$", "")
			if value ~= nil and value ~= "" then
				callback(value)
			elseif defaultValue ~= nil and defaultValue ~= "" then
				callback(defaultValue)
			end
		end,
	}):start()
end

local function load_connection_vars()
	loadConfigFromCommand(
		"op read op://personal/PostgreSQLConnection/connection_string --no-newline",
		"connection_string",
		function(conn_string)
			local env = parse_connection_string(conn_string)
			vim.env.PG_HOST = env.host
			vim.env.PG_PORT = env.port
			vim.env.PG_USER = env.user
			vim.env.PG_PASSWORD = env.password
			vim.env.PG_DATABASE = env.dbname
		end
	)
end
-- Use one password and the above loadConfigFromCommand to populate the env variable just like the function
-- load_env_variables does
-- op read op://personal/TavusProd/port --no-newline

-- Function to load environment variables
local function load_env_variables()
	load_connection_vars() -- Call this first to populate env variables

	local env = {
		host = vim.env.PG_HOST,
		port = vim.env.PG_PORT,
		user = vim.env.PG_USER,
		password = vim.env.PG_PASSWORD,
		database = vim.env.PG_DATABASE,
		psql_path = vim.env.PSQL_PATH or "psql",
	}
	for k, v in pairs(env) do
		if not v and k ~= "psql_path" then
			error("Environment variable " .. k .. " is not set")
		end
	end
	return env
end

-- Function to safely display messages
local function safe_notify(msg, level)
	vim.schedule(function()
		vim.notify(msg, level)
	end)
end

-- Function to execute query and return results
local function execute_query(query)
	local env = load_env_variables()
	local Job = require("plenary.job")
	local results = {}

	local job = Job:new({
		command = env.psql_path,
		args = {
			"-h",
			env.host,
			"-p",
			env.port,
			"-U",
			env.user,
			"-d",
			env.database,
			"-c",
			query,
			"-A", -- Unaligned table output
			"|", -- Field separator
			"--pset=footer=off", -- Don't show footer (row count)
		},
		env = { PGPASSWORD = env.password },
		on_exit = function(j, return_val)
			if return_val ~= 0 then
				local stderr = table.concat(j:stderr_result(), "\n")
				safe_notify("Error executing query: " .. stderr, vim.log.levels.ERROR)
				return
			end
			results = j:result()
		end,
	})

	job:sync() -- Wait for job to complete
	return results
end

-- Function to prettify JSON string
-- Replace the prettify_json function with this:
local function prettify_json(str)
	local success, parsed = pcall(json.decode, str)
	if success then
		return json.encode(parsed, { indent = 2 })
	end
	return str
end

-- Function to open detailed results in a new buffer with color and JSON formatting
local function open_detailed_results(entry)
	-- Create a new buffer
	local buf = api.nvim_create_buf(false, true)

	-- Generate a unique buffer name
	local buffer_number = 1
	local base_name = "PostgreSQL Result Details"
	local buf_name = base_name
	while vim.fn.bufexists(buf_name) ~= 0 do
		buffer_number = buffer_number + 1
		buf_name = base_name .. " " .. buffer_number
	end

	-- Set buffer name
	api.nvim_buf_set_name(buf, buf_name)

	-- Prepare content
	local lines = {}
	for i, col in ipairs(entry.value) do
		local header = entry.headers[i]
		local value = col

		-- Check if the value might be JSON
		if value:match("^%s*{") or value:match("^%s*%[") then
			value = prettify_json(value)
			if value ~= col then -- If JSON was successfully parsed and formatted
				table.insert(lines, string.format("%s:", header))
				for _, json_line in ipairs(vim.split(value, "\n")) do
					table.insert(lines, "  " .. json_line)
				end
			else
				table.insert(lines, string.format("%s: %s", header, value))
			end
		else
			table.insert(lines, string.format("%s: %s", header, value))
		end
	end

	-- Set buffer content
	api.nvim_buf_set_lines(buf, 0, -1, false, lines)

	-- Set buffer options
	api.nvim_buf_set_option(buf, "modifiable", true)
	api.nvim_buf_set_option(buf, "buftype", "nofile")

	-- Open buffer in a new window
	api.nvim_command("vsplit")
	api.nvim_win_set_buf(0, buf)

	-- Set keymaps
	local opts = { noremap = true, silent = true }
	api.nvim_buf_set_keymap(buf, "n", "q", ":bdelete<CR>", opts)

	-- Enable wrap and set large sidescrolloff for easy horizontal scrolling
	api.nvim_win_set_option(0, "wrap", false)
	api.nvim_win_set_option(0, "sidescrolloff", 15)

	-- Set up syntax highlighting
	vim.api.nvim_buf_set_option(buf, "syntax", "ON")
	vim.api.nvim_command([[
    syntax clear
    syntax match SqlResultHeader /^\w\+:/
    syntax match SqlResultValue /:\s\+\zs.*$/
    syntax region SqlJsonBlock start=/{/ end=/}/ contains=SqlJsonProperty,SqlJsonString,SqlJsonNumber fold
    syntax match SqlJsonProperty /"[^"]*":/
    syntax region SqlJsonString start=/"/ skip=/\\"/ end=/"/
    syntax match SqlJsonNumber /-\?\d\+\(\.\d\+\)\?\([eE][+-]\?\d\+\)\?/
    highlight SqlResultHeader ctermfg=Yellow guifg=#FFFF00
    highlight SqlResultValue ctermfg=LightBlue guifg=#ADD8E6
    highlight SqlJsonBlock ctermfg=Green guifg=#90EE90
    highlight SqlJsonProperty ctermfg=Cyan guifg=#00FFFF
    highlight SqlJsonString ctermfg=LightGreen guifg=#98FB98
    highlight SqlJsonNumber ctermfg=Magenta guifg=#FF00FF
  ]])

	-- Make buffer unmodifiable after setting up syntax
	api.nvim_buf_set_option(buf, "modifiable", false)
end

-- Function to create the Telescope UI
function M.show_results(results)
	if #results == 0 then
		safe_notify("No results returned", vim.log.levels.INFO)
		return
	end

	local headers = vim.split(results[1], "|")
	table.remove(results, 1) -- Remove header row from results

	pickers
		.new({}, {
			prompt_title = "PostgreSQL Query Results",
			finder = finders.new_table({
				results = results,
				entry_maker = function(entry)
					local columns = vim.split(entry, "|")
					local display_string = table.concat(columns, " | ")
					return {
						value = columns,
						display = display_string,
						ordinal = display_string,
						headers = headers,
					}
				end,
			}),
			sorter = conf.generic_sorter({}),
			attach_mappings = function(prompt_bufnr, map)
				actions.select_default:replace(function()
					actions.close(prompt_bufnr)
					local selection = action_state.get_selected_entry()
					open_detailed_results(selection)
				end)
				return true
			end,
		})
		:find()
end

-- Main function to run query and display results
function M.run_query()
	local query = api.nvim_buf_get_lines(0, 0, -1, false)
	query = table.concat(query, "\n")

	local results = execute_query(query)
	M.show_results(results)
end

return M
