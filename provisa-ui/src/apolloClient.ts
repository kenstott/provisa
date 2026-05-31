import { ApolloClient, InMemoryCache, HttpLink, ApolloLink } from '@apollo/client';

const API_BASE = import.meta.env.VITE_API_BASE || '';

const httpLink = new HttpLink({
  uri: `${API_BASE}/admin/graphql`,
  credentials: 'include',
});

const authLink = new ApolloLink((operation, forward) => {
  const token = localStorage.getItem('provisa_token');
  if (token) {
    operation.setContext({
      headers: {
        authorization: `Bearer ${token}`,
      },
    });
  }
  return forward(operation);
});

const cache = new InMemoryCache({
  typePolicies: {
    Query: {
      fields: {
        domains: {
          merge(_existing = [], incoming) {
            return incoming;
          },
        },
        tables: {
          merge(_existing = [], incoming) {
            return incoming;
          },
        },
        relationships: {
          merge(_existing = [], incoming) {
            return incoming;
          },
        },
        roles: {
          merge(_existing = [], incoming) {
            return incoming;
          },
        },
      },
    },
  },
});

if (typeof window !== 'undefined') {
  const stored = localStorage.getItem('apollo-cache');
  if (stored) {
    try {
      cache.restore(JSON.parse(stored));
    } catch (e) {
      console.warn('Failed to restore Apollo cache:', e);
    }
  }
}

export const client = new ApolloClient({
  ssrMode: typeof window === 'undefined',
  link: authLink.concat(httpLink),
  cache,
  defaultOptions: {
    watchQuery: {
      fetchPolicy: 'cache-and-network',
    },
    query: {
      fetchPolicy: 'cache-first',
    },
  },
});

if (typeof window !== 'undefined') {
  setInterval(() => {
    const cacheData = cache.extract();
    localStorage.setItem('apollo-cache', JSON.stringify(cacheData));
  }, 5000);
}
