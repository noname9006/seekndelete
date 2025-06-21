# SeekNDelete Bot

A Discord bot for server admins to quickly search and delete messages by content, user, and time.

## Features
- Search messages by content (case-insensitive by default)
- Optional: limit search by user or time window (e.g. "7d", "12h", "2d6h")
- Bulk deletion with confirmation and progress
- Abort/cancel running operations
- Admin-only commands

## Commands

### Search and Delete
```
>seekndelete "text to find" [@user] [max age]
```
- `"text to find"` (required): text to search for (in quotes)
- `@user` (optional): only search messages from this user
- `max age` (optional): e.g. `7d`, `12h`, `2d3h`

### Abort Operation
```
>seekndelete abort
```
Cancels all active seekndelete tasks in the current channel.

## Examples
```
>seekndelete "test"
>seekndelete "error" @someuser 2d
>seekndelete "update" 8h
>seekndelete abort
```

## Setup
1. Node.js v16+ required.
2. Create `.env` with:
   ```
   DISCORD_TOKEN=your-bot-token
   LOG_LEVEL=info
   ENABLE_FILE_LOGGING=false
   CASE_SENSITIVE_SEARCH=false
   ```
3. `npm install`
4. `node seekndelete.js`

**Warning:** All commands require ADMINISTRATOR permission. Double-check before confirming deletions!