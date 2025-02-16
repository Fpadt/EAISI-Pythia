library(ggplot2)
library(gganimate)
library(dplyr)

# PARAMETERS
n_stars   <- 100   # How many stars exist at any point in time
n_frames  <- 100   # Total frames in the animation
max_dist  <- 5     # Farthest distance (arbitrary units)
min_dist  <- 0.1   # Closest distance (where we'll remove the star)
speed     <- -0.05  # How quickly a star moves from max_dist to min_dist

set.seed(123)

# Create star birth times
star_births <- data.frame(
  star_id   = seq_len(n_stars),
  birthtime = sample(1:n_frames, n_stars, replace = TRUE)
)

make_star_trajectory <- function(star_id, birthtime) {
  angle <- runif(1, 0, 2*pi)
  x_dir <- cos(angle)
  y_dir <- sin(angle)
  
  # Sequence of frames from birth until end
  times <- seq(birthtime, n_frames)
  
  # Distance shrinks linearly over time
  dist_values <- max_dist - speed * (times - birthtime)
  
  # Keep only frames for which distance > min_dist
  times       <- times[dist_values > min_dist]
  dist_values <- dist_values[dist_values > min_dist]
  
  data.frame(
    star_id  = star_id,
    time     = times,            # rename "frame" -> "time"
    x        = x_dir * dist_values,
    y        = y_dir * dist_values,
    size     = 1 / dist_values,  
    alpha    = ifelse(
      test = dist_values >= (max_dist - 0.5),
      yes  = 1 - (max_dist - dist_values),     # fade in for the first 0.5 distance
      no   = ifelse(dist_values < 1, dist_values, 1) # fade out near the end
    )
  )
}

# Build a combined data set of star trajectories
star_data <- do.call(rbind, mapply(
  make_star_trajectory, 
  star_births$star_id, 
  star_births$birthtime,
  SIMPLIFY = FALSE
))

# Plot: use transition_time(time) instead of frame=
p <- ggplot(star_data, aes(x, y, group = star_id)) +
  geom_point(aes(size = size, alpha = alpha), color = "white") +
  scale_size_continuous(range = c(0.1, 6), guide = "none") +
  scale_alpha_continuous(range = c(0, 1), guide = "none") +
  coord_equal() +
  theme_void() +
  theme(
    plot.background  = element_rect(fill = "black", color = NA),
    panel.background = element_rect(fill = "black", color = NA)
  ) +
  labs(title = "Flying Through Space Demo") +
  transition_time(time) +            # tells gganimate how to transition
  ease_aes("linear")                 # linear transitions across frames

# Render the animation
anim <- animate(
  p,
  nframes     = n_frames,
  fps         = 10,
  width       = 500,
  height      = 500
)

# View in an interactive session
anim

# Optionally save:
anim_save("space_travel.gif", animation = anim)
