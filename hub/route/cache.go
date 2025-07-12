// This file is part of the Mihomo project: https://github.com/vernesong/mihomo
// Copyright (C) 2025 vernesong and contributors
//
// This file is licensed under the GNU General Public License v3.0.
// You may obtain a copy of the License at https://www.gnu.org/licenses/gpl-3.0.html

package route

import (
	"net/http"

	"github.com/metacubex/mihomo/component/profile/cachefile"
	"github.com/metacubex/mihomo/component/resolver"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/render"
)

func cacheRouter() http.Handler {
	r := chi.NewRouter()
	r.Post("/fakeip/flush", flushFakeIPPool)
	r.Post("/smart/flush", flushAllSmartCache)
	r.Post("/smart/flush/{config}", flushSmartConfigCache)
	return r
}

func flushFakeIPPool(w http.ResponseWriter, r *http.Request) {
	err := resolver.FlushFakeIP()
	if err != nil {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError(err.Error()))
		return
	}
	render.NoContent(w, r)
}

func flushAllSmartCache(w http.ResponseWriter, r *http.Request) {
	db := cachefile.Cache()
	if db == nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError("cache store not available"))
		return
	}

	smartStore := cachefile.NewSmartStore(db)
	if smartStore == nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError("smart store not available"))
		return
	}

	if err := smartStore.GetStore().FlushAll(); err != nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError(err.Error()))
		return
	}

	render.NoContent(w, r)
}

func flushSmartConfigCache(w http.ResponseWriter, r *http.Request) {
	configName := chi.URLParam(r, "config")
	if configName == "" {
		render.Status(r, http.StatusBadRequest)
		render.JSON(w, r, newError("config name is required"))
		return
	}

	db := cachefile.Cache()
	if db == nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError("cache store not available"))
		return
	}

	smartStore := cachefile.NewSmartStore(db)
	if smartStore == nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError("smart store not available"))
		return
	}

	if err := smartStore.GetStore().FlushByConfig(configName); err != nil {
		render.Status(r, http.StatusInternalServerError)
		render.JSON(w, r, newError(err.Error()))
		return
	}

	render.NoContent(w, r)
}
