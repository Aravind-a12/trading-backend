# NextAuth Configuration for Trading Backend

## 🔧 **NextAuth Configuration**

Your frontend needs to be configured to authenticate with your backend. Here's the correct configuration:

### **1. NextAuth Configuration File**

Create or update `pages/api/auth/[...nextauth].js` (or `app/api/auth/[...nextauth]/route.js` for App Router):

```javascript
import NextAuth from 'next-auth'
import CredentialsProvider from 'next-auth/providers/credentials'

export default NextAuth({
  providers: [
    CredentialsProvider({
      name: 'credentials',
      credentials: {
        email: { label: "Email", type: "email" },
        password: { label: "Password", type: "password" }
      },
      async authorize(credentials) {
        try {
          // Call your backend login endpoint
          const response = await fetch('http://localhost:8000/api/auth/login', {
            method: 'POST',
            headers: {
              'Content-Type': 'application/json',
            },
            body: JSON.stringify({
              email: credentials.email,
              password: credentials.password,
            }),
          })

          if (response.ok) {
            const data = await response.json()
            return {
              id: data.user.id,
              email: data.user.email,
              username: data.user.username,
              accessToken: data.access_token,
            }
          } else {
            const error = await response.json()
            console.error('Login failed:', error)
            return null
          }
        } catch (error) {
          console.error('Authentication error:', error)
          return null
        }
      }
    })
  ],
  callbacks: {
    async jwt({ token, user }) {
      // Persist the access token to the token right after signin
      if (user) {
        token.accessToken = user.accessToken
        token.id = user.id
        token.username = user.username
      }
      return token
    },
    async session({ session, token }) {
      // Send properties to the client
      session.accessToken = token.accessToken
      session.user.id = token.id
      session.user.username = token.username
      return session
    }
  },
  pages: {
    signIn: '/auth/signin', // Custom sign-in page
  },
  session: {
    strategy: 'jwt',
  },
  secret: process.env.NEXTAUTH_SECRET,
})
```

### **2. Environment Variables**

Add to your `.env.local`:

```env
NEXTAUTH_URL=http://localhost:3000
NEXTAUTH_SECRET=your-nextauth-secret-key-here
```

### **3. API Key Management with NextAuth**

Update your API key management to use the NextAuth session:

```javascript
import { useSession } from 'next-auth/react'

export default function ApiKeyManager() {
  const { data: session } = useSession()

  const createApiKey = async (apiKeyData) => {
    try {
      const response = await fetch('http://localhost:8000/api/keys', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${session.accessToken}`,
        },
        body: JSON.stringify(apiKeyData),
      })

      if (response.ok) {
        const data = await response.json()
        console.log('API key created:', data)
        return data
      } else {
        const error = await response.json()
        throw new Error(error.detail || 'Failed to create API key')
      }
    } catch (error) {
      console.error('Error creating API key:', error)
      throw error
    }
  }

  // ... rest of your component
}
```

### **4. Sign In Component**

Update your sign-in component:

```javascript
import { signIn, getSession } from 'next-auth/react'

export default function SignIn() {
  const handleSubmit = async (e) => {
    e.preventDefault()
    
    const result = await signIn('credentials', {
      email: e.target.email.value,
      password: e.target.password.value,
      redirect: false,
    })

    if (result?.error) {
      console.error('Sign in failed:', result.error)
      // Handle error
    } else {
      // Success - redirect or update UI
      window.location.href = '/dashboard'
    }
  }

  return (
    <form onSubmit={handleSubmit}>
      <input name="email" type="email" required />
      <input name="password" type="password" required />
      <button type="submit">Sign In</button>
    </form>
  )
}
```

## 🚨 **Important Notes**

1. **Backend URL**: Make sure your backend is running on `http://localhost:8000`
2. **CORS**: Your backend already has CORS configured for `http://localhost:3000`
3. **Token Storage**: NextAuth will handle JWT token storage automatically
4. **Session Management**: Use `useSession()` hook to access user data and tokens

## 🔍 **Debugging Steps**

1. **Check Backend**: Ensure your backend is running and accessible
2. **Check NextAuth Config**: Verify the configuration matches your backend endpoints
3. **Check Environment Variables**: Ensure `NEXTAUTH_SECRET` is set
4. **Check Network**: Use browser dev tools to see the actual requests being made

## 📝 **Testing**

1. Start your backend: `python start.py`
2. Start your frontend: `npm run dev`
3. Try to sign in with valid credentials
4. Check browser network tab for any errors
