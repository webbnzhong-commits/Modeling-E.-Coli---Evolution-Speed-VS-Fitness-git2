# pip install pillow numpy scipy

from PIL import Image, ImageDraw, ImageFont
import numpy as np
from scipy.ndimage import distance_transform_edt

def smoothstep(edge0, edge1, x):
    # Smoothly goes 0->1 between edge0 and edge1
    t = np.clip((x - edge0) / (edge1 - edge0 + 1e-9), 0.0, 1.0)
    return t * t * (3 - 2 * t)

def lerp(a, b, t):
    return a * (1 - t) + b * t

def render_ocrb_hello_world(
    out_path="hello_world_ocrb.png",
    text="hello world",
    font_path="OCR-B.ttf",        # <-- set this to your OCR-B .ttf path
    font_size=140,
    padding=60,
    scale=4,                      # render big then downsample for crisp edges
    base_rgb=(47, 21, 12),
    edge_rgb=(45, 30, 29),
    inner_core_px=18,             # how deep from edge to reach “true” base color
    outer_fade_px=10,             # how quickly it fades to white outside the text
    jitter_strength=6,            # random color variation amount near center
    seed=1
):
    rng = np.random.default_rng(seed)

    # --- Load font (fallback if not found) ---
    try:
        font = ImageFont.truetype(font_path, font_size * scale)
    except OSError:
        # If OCR-B isn't found, this will still run, but you won't get OCR-B.
        font = ImageFont.load_default()

    # --- Measure text ---
    tmp = Image.new("L", (1, 1), 0)
    dtmp = ImageDraw.Draw(tmp)
    bbox = dtmp.textbbox((0, 0), text, font=font)
    tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]

    W = tw + 2 * padding * scale
    H = th + 2 * padding * scale

    # --- Create text mask (white text on black bg) ---
    mask_img = Image.new("L", (W, H), 0)
    draw = ImageDraw.Draw(mask_img)
    x = padding * scale - bbox[0]
    y = padding * scale - bbox[1]
    draw.text((x, y), text, fill=255, font=font)

    # --- Distance fields ---
    mask = (np.array(mask_img) > 0)
    dist_in = distance_transform_edt(mask)          # inside: 0 at edge, larger toward center
    dist_out = distance_transform_edt(~mask)        # outside: 0 at edge, larger away from text

    # --- Build color field ---
    base = np.array(base_rgb, dtype=np.float32)
    edge = np.array(edge_rgb, dtype=np.float32)
    white = np.array((255, 255, 255), dtype=np.float32)

    # Inside blend: edge -> base (+ jitter) as you go inward
    w_in = smoothstep(0.0, inner_core_px * scale, dist_in)  # 0 near edge, 1 deeper inside

    # Random jitter mostly in the center (scaled by w_in)
    jitter = rng.normal(0, 1, size=(H, W, 3)).astype(np.float32)
    jitter = jitter * (jitter_strength * w_in[..., None])
    base_jittered = np.clip(base + jitter, 0, 255)

    inside_rgb = lerp(edge[None, None, :], base_jittered, w_in[..., None])

    # Outside blend: edge -> white quickly as you move outward
    w_out = smoothstep(0.0, outer_fade_px * scale, dist_out)  # 0 at edge, 1 farther out
    outside_rgb = lerp(edge[None, None, :], white[None, None, :], w_out[..., None])

    rgb = np.where(mask[..., None], inside_rgb, outside_rgb).astype(np.uint8)

    # --- Convert to image and downsample for nicer antialiasing ---
    img = Image.fromarray(rgb, mode="RGB")
    img = img.resize((W // scale, H // scale), resample=Image.Resampling.LANCZOS)
    img.save(out_path)
    print(f"Saved: {out_path}")

if __name__ == "__main__":
    # Set font_path to your OCR-B TTF file location, e.g.:
    # font_path="/path/to/OCR-B.ttf"
    render_ocrb_hello_world(
        out_path="hello_world_ocrb.png",
        font_path="OCR-B.ttf"
    )