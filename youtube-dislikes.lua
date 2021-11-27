dofile("table_show.lua")
dofile("urlcode.lua")
local urlparse = require("socket.url")
local http = require("socket.http")
JSON = (loadfile "JSON.lua")()

local item_dir = os.getenv('item_dir')
local warc_file_base = os.getenv('warc_file_base')
local visitor_data = os.getenv('visitor_data')
local item_type = nil
local item_name = nil
local item_value = nil

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false

local bad_items = {}

local video_ids = {}

if not urlparse or not http then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

for video_id in string.gmatch(os.getenv('ids'), "([^,]+)") do
  video_ids[video_id] = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}

  if not string.find(url, "youtube") then
    for video_id, _ in pairs(video_ids) do
      table.insert(urls, {
        url="https://www.youtube.com/youtubei/v1/next?key=AIzaSyAO_FJ2SlqU8Q4STEHLGCilw_Y9_11qcW8&video_id=" .. video_id,
        method="POST",
        body_data=JSON:encode({
          context={
            client={
              hl="en",
              gl="US",
              clientName="WEB",
              clientVersion="2.20210101",
            },
          },
          videoId=video_id
        }),
        headers={
          ["Referer"]=nil,
          ["Content-Type"]="application/json",
          ["X-Goog-Visitor-Id"]=visitor_data,
          ["Accept-Language"]="en-US;q=0.9, en;q=0.8"
        }
      })
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  if not string.find(url["url"], "youtube") then
    return false
  end
  local data = JSON:decode(read_file(http_stat["local_file"]))
  local video_id = string.match(url["url"], "video_id=([^&%?=]+)$")
  local dislike_data = data["contents"]["twoColumnWatchNextResults"]["results"]["results"]["contents"]
  if not dislike_data then
    io.stdout:write("Something went wrong getting data.\n")
    io.stdout:flush()
    bad_items[video_id] = true
    return false
  end
  for _, d in pairs(dislike_data) do
    d = d["videoPrimaryInfoRenderer"]
    if d then
      dislike_data = d["videoActions"]["menuRenderer"]["topLevelButtons"]
      break
    end
  end
  dislike_data = dislike_data[2]
  if not dislike_data then
    io.stdout:write("Video not available (?).\n")
    io.stdout:flush()
    bad_items[video_id] = true
    return false
  end
  dislike_data = dislike_data["toggleButtonRenderer"]
  if not string.match(dislike_data["accessibility"]["label"], "^dislike this video along with [0-9,]+ other pe")
    or not string.match(dislike_data["defaultText"]["accessibility"]["accessibilityData"]["label"], "^[0-9,]*N?o? dislikes?$") then
    io.stdout:write("Found bad dislike data.\n")
    io.stdout:flush()
    bad_items[video_id] = true
    return false
  end
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if not string.find(url["url"], "youtube") then
    return wget.actions.NOTHING
  end
  
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. "  \n")
  io.stdout:flush()

  if status_code ~= 200 then
    return wget.actions.ABORT
  end

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local file = io.open(item_dir .. '/' .. warc_file_base .. '_bad-items.txt', 'w')
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if abortgrab then
    return wget.exits.IO_FAIL
  end
  return exit_status
end

