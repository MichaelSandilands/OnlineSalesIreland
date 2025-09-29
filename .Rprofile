# Activate renv
source("renv/activate.R")

# --- AI Tool Setup ---
# Check if {chores}, {gander} and {ellmer} are installed and an API key is present
if (requireNamespace("chores", quietly = TRUE) &&
    requireNamespace("gander", quietly = TRUE) &&
    requireNamespace("ellmer", quietly = TRUE) &&
    Sys.getenv("ANTHROPIC_API_KEY") != "") {

  # Set the {chores} chat option to use the {ellmer} OpenAI chat model
  options(.chores_chat = ellmer::chat_anthropic())
  options(.gander_chat = ellmer::chat_anthropic())
}

# Load {devtools} if it is installed for development functions (e.g., check, document, load_all)
if (requireNamespace("devtools", quietly = TRUE)) {
  library(devtools)
}
