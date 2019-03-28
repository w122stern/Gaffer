package uk.gov.gchq.gaffer.federatedstore;

import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.store.StoreProperties;

public class FederatedStorePropertiesUtil {
    /**
     * This is used....
     * e.g gaffer.federatedstore.isPublicAllowed=true
     */
    public static final String IS_PUBLIC_ACCESS_ALLOWED = "gaffer.federatedstore.isPublicAllowed";
    public static final String IS_PUBLIC_ACCESS_ALLOWED_DEFAULT = String.valueOf(true);
    /**
     * This is used....
     * e.g gaffer.federatedstore.customPropertiesAuths="auth1"
     */
    public static final String CUSTOM_PROPERTIES_AUTHS = "gaffer.federatedstore.customPropertiesAuths";
    public static final String CUSTOM_PROPERTIES_AUTHS_DEFAULT = null;
    /**
     * This is used....
     * eg.gaffer.federatedstore.cache.service.class="uk.gov.gchq.gaffer.cache.impl.HashMapCacheService"
     */
    public static final String CACHE_SERVICE_CLASS = CacheProperties.CACHE_SERVICE_CLASS;
    public static final String CACHE_SERVICE_CLASS_DEFAULT = null;

    public static void setCustomPropertyAuths(final StoreProperties federatedStoreProperties, final String auths) {
        federatedStoreProperties.setProperty(CUSTOM_PROPERTIES_AUTHS, auths);
    }

    public static void setCacheProperties(final StoreProperties federatedStoreProperties, final String cacheServiceClassString) {
        federatedStoreProperties.setProperty(CACHE_SERVICE_CLASS, cacheServiceClassString);
    }

    public static String getCacheProperties(final StoreProperties federatedStoreProperties) {
        return federatedStoreProperties.getProperty(CACHE_SERVICE_CLASS, CACHE_SERVICE_CLASS_DEFAULT);
    }

    public static String getCustomPropsValue(final StoreProperties federatedStoreProperties) {
        return federatedStoreProperties.getProperty(CUSTOM_PROPERTIES_AUTHS, CUSTOM_PROPERTIES_AUTHS_DEFAULT);
    }

    public static String getIsPublicAccessAllowed(final StoreProperties federatedStoreProperties) {
        return federatedStoreProperties.getProperty(IS_PUBLIC_ACCESS_ALLOWED, IS_PUBLIC_ACCESS_ALLOWED_DEFAULT);
    }

    public static void setFalseGraphsCanHavePublicAccess(final StoreProperties federatedStoreProperties) {
        setGraphsCanHavePublicAccess(federatedStoreProperties, false);
    }

    public static void setTrueGraphsCanHavePublicAccess(final StoreProperties federatedStoreProperties) {
        setGraphsCanHavePublicAccess(federatedStoreProperties, true);
    }

    public static void setGraphsCanHavePublicAccess(final StoreProperties federatedStoreProperties, final boolean b) {
        federatedStoreProperties.setProperty(IS_PUBLIC_ACCESS_ALLOWED, Boolean.toString(b));
    }
}