import numpy as np

try:
    import numba as nb

    HAS_NUMBA = True
except Exception:
    HAS_NUMBA = False


if HAS_NUMBA:

    @nb.njit(cache=True)
    def _fast_update_nb(
        realx,
        realy,
        speedx,
        speedy,
        life_cycles,
        max_cycles,
        res_o,
        res_c,
        res_n,
        repro_o,
        repro_c,
        repro_n,
        opt_ph,
        opt_temp,
        evo_speed,
        target_evo_speed,
        evo_speed_min,
        evo_speed_max,
        ph_level,
        temp,
        ph_scale,
        ph_div,
        temp_scale,
        temp_div,
        repro_debuf_min,
        width,
        height,
        pool_o,
        pool_c,
        pool_n,
    ):
        n = realx.shape[0]
        dead = np.zeros(n, np.bool_)
        reproduce = np.zeros(n, np.bool_)
        if ph_div <= 0:
            ph_div = 1e-6
        if temp_div <= 0:
            temp_div = 1e-6
        for i in range(n):
            realx[i] += speedx[i]
            realy[i] += speedy[i]
            x = int(realx[i])
            y = int(realy[i])
            if x <= 0 or x >= width:
                speedx[i] *= -1.0
            if y <= 0 or y >= height:
                speedy[i] *= -1.0

            life_cycles[i] += 1
            if life_cycles[i] >= max_cycles[i]:
                dead[i] = True
                continue

            if pool_o > 0:
                res_o[i] += pool_o
            if pool_c > 0:
                res_c[i] += pool_c
            if pool_n > 0:
                res_n[i] += pool_n

            ph_diff = opt_ph[i] - ph_level
            if ph_diff < 0:
                ph_diff = -ph_diff
            temp_diff = opt_temp[i] - temp
            if temp_diff < 0:
                temp_diff = -temp_diff
            ph_effect = (ph_diff / ph_div) * ph_scale
            if ph_effect > 1.0:
                ph_effect = 1.0
            temp_effect = (temp_diff / temp_div) * temp_scale
            if temp_effect > 1.0:
                temp_effect = 1.0
            debuf = ph_effect * temp_effect
            if debuf < repro_debuf_min:
                debuf = repro_debuf_min

            evo_span = evo_speed_max - evo_speed_min
            if evo_span <= 0.0:
                evo_span = 1e-6
            evo_norm = (evo_speed[i] - evo_speed_min) / evo_span
            if evo_norm < 0.0:
                evo_norm = 0.0
            if evo_norm > 1.0:
                evo_norm = 1.0
            mutational_load = 1.0 + 0.22 * (evo_norm * evo_norm)
            debuf *= mutational_load

            if (res_o[i] / debuf >= repro_o[i]
                and res_c[i] / debuf >= repro_c[i]
                and res_n[i] / debuf >= repro_n[i]):
                reproduce[i] = True
                res_o[i] -= repro_o[i]
                res_c[i] -= repro_c[i]
                res_n[i] -= repro_n[i]

        return dead, reproduce



def _fast_update_np(
    realx,
    realy,
    speedx,
    speedy,
    life_cycles,
    max_cycles,
    res_o,
    res_c,
    res_n,
    repro_o,
    repro_c,
    repro_n,
    opt_ph,
    opt_temp,
    evo_speed,
    target_evo_speed,
    evo_speed_min,
    evo_speed_max,
    ph_level,
    temp,
    ph_scale,
    ph_div,
    temp_scale,
    temp_div,
    repro_debuf_min,
    width,
    height,
    pool_o,
    pool_c,
    pool_n,
):
    realx[:] = realx + speedx
    realy[:] = realy + speedy
    x = realx.astype(np.int32)
    y = realy.astype(np.int32)
    bounce_x = (x <= 0) | (x >= width)
    bounce_y = (y <= 0) | (y >= height)
    speedx[bounce_x] *= -1.0
    speedy[bounce_y] *= -1.0

    life_cycles[:] = life_cycles + 1
    dead = life_cycles >= max_cycles

    if pool_o > 0:
        res_o[:] = res_o + pool_o
    if pool_c > 0:
        res_c[:] = res_c + pool_c
    if pool_n > 0:
        res_n[:] = res_n + pool_n

    ph_div = ph_div if ph_div > 0 else 1e-6
    temp_div = temp_div if temp_div > 0 else 1e-6
    ph_effect = np.minimum(1.0, (np.abs(opt_ph - ph_level) / ph_div) * ph_scale)
    temp_effect = np.minimum(1.0, (np.abs(opt_temp - temp) / temp_div) * temp_scale)
    debuf = np.maximum(repro_debuf_min, ph_effect * temp_effect)

    evo_span = float(evo_speed_max) - float(evo_speed_min)
    if evo_span <= 0.0:
        evo_span = 1e-6
    evo_norm = np.clip((evo_speed - float(evo_speed_min)) / evo_span, 0.0, 1.0)
    mutational_load = 1.0 + 0.22 * np.square(evo_norm)
    debuf = debuf * mutational_load

    reproduce = (
        (res_o / debuf >= repro_o)
        & (res_c / debuf >= repro_c)
        & (res_n / debuf >= repro_n)
        & (~dead)
    )
    if np.any(reproduce):
        res_o[reproduce] -= repro_o[reproduce]
        res_c[reproduce] -= repro_c[reproduce]
        res_n[reproduce] -= repro_n[reproduce]
    return dead, reproduce



def fast_update(*args, **kwargs):
    if HAS_NUMBA:
        return _fast_update_nb(*args, **kwargs)
    return _fast_update_np(*args, **kwargs)
